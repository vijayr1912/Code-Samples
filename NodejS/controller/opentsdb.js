/**
 * @file lib/datastore/opentsdb.js
 * Provides data storage and retrieval for the IPM web application in an
 * OpenTSDB cluster
 */

(function() {
    "use strict";

    /**
     * Module dependencies.
     */
    var net = require('net');
    var request = require('superagent');
    var StatsD = require('node-statsd');
    var TsdbError = require('errors').TsdbError;

    var log = require('log').namespace('opentsdb');

    /**
     * A copy of the configuration object
     *
     * @name localConfig
     * @memberof datastore/opentsdb
     * @private
     */
    var localConfig;

    /**
     * The socket connection to OpenTSDB
     *
     * @name opentsdb
     * @memberof datastore/opentsdb
     * @private
     */
    var opentsdb;

    /**
     * Statsd client, to record performance statistics
     *
     * @name statsClient
     * @memberof datastore/opentsdb
     * @private
     */
    var statsClient = new StatsD();

    /**
     * Convert a hash of tags into a string
     *
     * @name hashToString
     * @memberof datastore/opentsdb
     * @function
     * @private
     * @param obj {Object} The hash to convert
     * @return {String} A string of the form "key1=value1,key2=value2"
     */
    var hashToString = function(obj) {
        var retval = '';
        for (var field in obj) {
            if (obj.hasOwnProperty(field) && typeof(obj[field]) === 'string') {
                if (retval !== '') {
                    retval += ',';
                }
                retval += field + '=' + obj[field];
            }
        }
        return retval;
    };

    /**
     * Log a request to OpenTSDB.
     *
     * @name logTsdbRequest
     * @memberof datastore/opentsdb
     * @function
     * @private
     * @param uri {String} The OpenTSDB request URL to be logged
     */
    var logTsdbRequest = function(uri) {
        log.info('Outgoing TSDB request: ' + uri);
    };

    /**
     * Connect to the time series database.
     *
     * @name connect
     * @memberof datastore/opentsdb
     * @function
     * @public
     * @param nconf {Object} The application configuration
     * @param callback {Function} Callback to fire when finished, with err and
     *                            socket parameters. The socket parameter is a
     *                            net.Socket object representing the OpenTSDB
     *                            connection.
     */
    var connect = function(nconf, callback) {
        localConfig = nconf;

        // Callback for connection errors
        function connectionError(err) {
            // Record failure in statsd
            statsClient.increment('opentsdb.failure');

            log.fatal('OpenTSDB connection error: ' + err);
            process.exit(1);
        }

        // Callback for a successful connection
        function connectionEstablished() {
            log.info('Connected to OpenTSDB.');

            opentsdb = socket;

            // Handle errors on this connection
            opentsdb.addListener('error', function(err) {
                log.error('OpenTSDB  network error: ' + err);
                opentsdb.destroy();
                opentsdb = undefined; // Cause a reconnection
            });
            opentsdb.removeListener('error', connectionError);

            // Allow this socket to be destroyed
            opentsdb.unref();

            callback(null, socket);
        }

        // Connect to OpenTSDB
        try {
            var socket = net.createConnection(
                localConfig.get('opentsdb:port'),
                localConfig.get('opentsdb:host'),
                connectionEstablished
            );

            // Handle errors when connecting
            socket.once('error', connectionError);
        } catch(err) {
            connectionError(err);
        }
    };

    /**
     * Request a range of metrics with the supplied name and tags between the
     * supplied start and end times
     *
     * @name queryRange
     * @memberof datastore/opentsdb
     * @function
     * @private
     * @param metric {String} The name of the time series
     * @param filter {Object} Filtering parameters for the metric:
     *     start {moment} The start time that the range should include
     *     end {moment} The end time that the range should include
     *     timezone {String} The time zone, e.g. 'Australia/Adelaide'
     *                       (currently unused; waiting for OpenTSDB 2.3)
     *     aggregation {String} The aggregation function to apply to the time series
     *                          E.g. 'avg', 'max'
     *     qualifiers {Array} An array of qualifiers (such as rate, downsampling etc.)
     *                        May be empty.
     *     tags {Object}
     * @param callback {Function} Callback to fire when finished, with err and
     *                            result parameters. The result parameter is
     *                            an array structure from the parsed JSON
     *                            response. This includes an object
     *                            corresponding to the requested metric.
     *                            Notably, this object has a 'dps' field that
     *                            is a mapping from timestamp (string of
     *                            value in seconds) to metric value (number).
     */
    var queryRange = function(metric, filter, callback) {
        // Build the URI to query OpenTSDB
        var uri = 'http://' + localConfig.get('opentsdb:host') + ':' +
            localConfig.get('opentsdb:port') +
            '/api/query?start=' + Math.floor(filter.start.unix()) +
            '&end=' + Math.ceil(filter.end.unix()) +
            '&m=' + filter.aggregation + ':';

        if (filter.qualifiers && filter.qualifiers.length) {
            uri += filter.qualifiers.join(':') + ':';
        }

        uri += metric;

        // Any tags supplied?
        if (Object.keys(filter.tags).length) {
            uri += '{' + hashToString(filter.tags) + '}';
        }

        // Perform the HTTP request
        var startTime = Date.now();
        logTsdbRequest(uri);
        request.get(uri).end(function(err, res) {
            // Check for server/transport errors
            if (err || res.error) {
                // OpenTSDB throws a 'Bad Request' if the HPC ID is not known
                if (res && (res.error.status === 400 ||
                    (res.error.status === 500 &&
                     res.body && res.body.error && res.body.error.message &&
                     res.body.error.message.indexOf('No such name for ') > -1))) {
                    // Record the time to process the request in statsd.
                    // NB: Date.now() is wall-clock time, and so can have
                    // jumps at arbitrary times (e.g. NTP changes), and so
                    // won't always give accurate elapsed time.
                    statsClient.timing('opentsdb.query.queryRange.time', Date.now() - startTime);

                    callback(null, []);
                } else {
                    // A real error occurred
                    log.error('Error reading from OpenTSDB ' + uri + ': ' + err || res.error.message);

                    // Record failure in statsd
                    statsClient.increment('opentsdb.query.queryRange.failure');

                    callback(new TsdbError(err || res.error.message));
                }
            } else {
                // Record the time to process the request in statsd
                statsClient.timing('opentsdb.query.queryRange.time', Date.now() - startTime);

                if (res.body) {
                    // Return the results
                    callback(null, res.body);
                } else {
                    callback(null, []);
                }
            }
        });
    };

    /**
     * Convert an OpenTSDB response into an array of points.
     *
     * @name responseToLines
     * @memberof datastore/opentsdb
     * @function
     * @private
     * @param dps {Object} An object where each key is a string of a number
     *                     representing the timestamp of the point (in seconds
     *                     since the Unix Epoch),
     *                     and each value is the metric value for that point
     * @return {Array} An array of point objects with fields:
     *                 - timestamp {Number} Timestamp in seconds since the Unix
     *                                      Epoch
     *                 - value {Number} Point value
     */
    var responseToLines = function(dps) {
        return Object.keys(dps).sort().map(function(key) {
            // Convert string timestamp key to a number.
            return { timestamp: parseInt(key), value: dps[key] };
        });
    };

    /**
     * @namespace Data storage and retrieval for our application
     * @name datastore
     */
    module.exports = {
        /**
         * Connect to the time series database.
         *
         * @name connect
         * @memberof datastore/opentsdb
         * @function
         * @public
         * @param nconf {Object} The application configuration
         * @param callback {Function} Callback to fire when finished, with
         *                            error and socket parameters. The socket
         *                            parameter is a net.Socket object
         *                            representing the OpenTSDB connection.
         */
        connect: connect,

        /**
         * Add an entry to the specified time series
         *
         * @name addTimeSeries
         * @memberof datastore/opentsdb
         * @function
         * @public
         * @param metric {Object} The Metric model object to store
         * @param callback {Function} Callback to fire when finished,
         *                            with err parameter
         */
        addTimeSeries: function(metric, callback) {
            var startTime = Date.now();

            // Send the data to OpenTSDB
            function sendTimeSeries(err) {
                if (err) {
                    callback(err);
                } else {
                    // put <name> <seconds_timestamp> <value>
                    if (!opentsdb.write('put ' + metric.toString() + '\n', 'ascii', callback)) {
                        // Record failure in statsd
                        statsClient.increment('opentsdb.write.delayed');

                        // This should not be a problem unless data is queued for too long
                        log.info('OpenTSDB data queued for writing');
                    } else {
                        // Record the time to process the request in statsd
                        statsClient.timing('opentsdb.write.time', Date.now() - startTime);
                    }
                }
            }

            // Check whether we need to re-open the connection to OpenTSDB
            if (!opentsdb || !opentsdb.writable) {
                // Re-open and send
                connect(localConfig, sendTimeSeries);
            } else {
                // Connection is open, just send
                sendTimeSeries();
            }
        },

        /**
         * Get the latest entry from the specified time series
         *
         * @name getLatestTimeSeries
         * @memberof datastore/opentsdb
         * @function
         * @public
         * @param metric {String} The name of the time series
         * @param tags {Object}
         * @param callback {Function} Callback to fire when finished, with err
         *                            and result parameters.
         *                            Note that err will not be set if there was
         *                            a 400 error or a '500 No such name for'
         *                            error response from OpenTSDB.
         *                            The result parameter is an object with:
         *                            - timestamp {Number} seconds since the
         *                                                 Unix Epoch
         *                            - value {Number}
         *                            The result may be undefined.
         */
        getLatestTimeSeries: function(metric, tags, callback) {
            // Build the URI to query OpenTSDB
            var uri = 'http://' + localConfig.get('opentsdb:host') + ':' +
                localConfig.get('opentsdb:port') +
                '/api/query/last?back_scan=96&timeseries=' +
                metric;

            // Any tags supplied?
            if (Object.keys(tags).length) {
                uri += '{' + hashToString(tags) + '}';
            }

            // Perform the HTTP request
            var startTime = Date.now();
            logTsdbRequest(uri);
            request.get(uri).end(function(err, res) {
                // Check for server/transport errors
                if (err || res.error) {
                    // OpenTSDB throws a 'Bad Request' if the HPC ID is not known, which
                    // is (sometimes) reported as HTTP 500, when it should be HTTP 400 or 404.
                    if (res &&
                        ((res.error.status === 400 ||
                         res.error.status === 500) &&
                         res.body && res.body.error && res.body.error.message &&
                         res.body.error.message.indexOf('No such name for ') > -1)) {
                        // Record the time to process the request in statsd
                        statsClient.timing('opentsdb.query.getLatestTimeSeries.time', Date.now() - startTime);

                        callback();
                    } else {
                        // A real error occurred
                        log.error('Error reading from OpenTSDB ' + uri + ': ' + err || res.error.message);

                        // Record failure in statsd
                        statsClient.increment('opentsdb.query.getLatestTimeSeries.failure');

                        callback(new TsdbError(err || res.error.message));
                    }
                } else {
                    // Record the time to process the request in statsd
                    statsClient.timing('opentsdb.query.getLatestTimeSeries.time', Date.now() - startTime);

                    if (res.body && res.body[0]) {
                        var point = res.body[0];

                        // OpenTSDB's /api/query returns timestamps in milliseconds
                        callback(null, { timestamp: Number(point.timestamp / 1000).toFixed(0), value: point.value });
                    } else {
                        callback();
                    }
                }
            });
        },

        /**
         * Get a range of metrics with the supplied name and tags between the
         * supplied start and end times
         *
         * @name getTimeSeriesRange
         * @memberof datastore/opentsdb
         * @function
         * @public
         * @param metric {String} The name of the time series
         * @param filter {Object} Filtering parameters for the metric
         *     start {moment} The start time that the range should include
         *     end {moment} The end time that the range should include
         *     timezone {String} The time zone, e.g. 'Australia/Adelaide'
         *                       (currently unused; waiting for OpenTSDB 2.3)
         *     aggregation {String} The aggregation function to apply to the time series
         *                          E.g. 'avg', 'max'
         *     qualifiers {Array} An array of qualifiers (such as rate, downsampling etc.)
         *                        May be empty.
         *     tags {Object}
         * @param callback {Function} Callback to fire when finished, with err
         *                            and result parameters.
         *                            The result parameter is an array of point
         *                            objects with fields:
         *                            - timestamp {Number} Timestamp in seconds
         *                              since the Unix Epoch
         *                            - value {Number} Point value
         */
        getTimeSeriesRange: function(metric, filter, callback) {
            queryRange(metric, filter, function(err, body) {
                if (!err && body.length === 1) {
                    // Return the results
                    callback(null, responseToLines(body[0].dps));
                } else {
                    callback(err, []);
                }
            });
        },

        /**
         * Get a set of ranges of metrics with the supplied name and tags between the
         * supplied start and end times
         *
         * @name getTimeSeriesRangeList
         * @memberof datastore/opentsdb
         * @function
         * @public
         * @param metric {String} The name of the time series
         * @param filter {Object} Filtering parameters for the metric
         *     start {moment} The start time that the range should include
         *     end {moment} The end time that the range should include
         *     timezone {String} The time zone, e.g. 'Australia/Adelaide'
         *                       (currently unused; waiting for OpenTSDB 2.3)
         *     aggregation {String} The aggregation function to apply to the time series
         *                          E.g. 'avg', 'max'
         *     qualifiers {Array} An array of qualifiers (such as rate, downsampling etc.)
         *         May be empty.
         *     tags {Object}
         * @param group {String} The name of the tag that should be used for grouping
         * @param callback {Function} Callback to fire when finished, with err
         *                            and result parameters.
         *                            The result parameter is an object mapping
         *                            the grouping filter tag value to an array
         *                            of point objects with fields:
         *                            - timestamp {Number} Timestamp in seconds
         *                              since the Unix Epoch
         *                            - value {Number} Point value
         */
        getTimeSeriesRangeList: function(metric, filter, group, callback) {
            // Ensure that the group tag is present in the filter
            if (!filter.tags) {
                filter.tags = {};
            }
            if (!filter.tags[group]) {
                filter.tags[group] = '*';
            }

            // Perform the query
            queryRange(metric, filter, function(err, body) {
                var retval = {};

                if (!err) {
                    // Process the result into an object
                    body.forEach(function(series) {
                        retval[series.tags[group]] = responseToLines(series.dps);
                    });
                }

                callback(err, retval);
            });
        },

        /**
         * Rename the tag value in time series database
         *
         * @name renameTagValue
         * @memberof datastore.opentsdb
         * @function
         * @public
         * @param oldname {String} The old tagv name value
         * @param newname {String} The new tagv name value
         * @param callback {Function} Callback to fire when finished, with err
         *                            parameter
         */
        renameTagValue: function(oldname, newname, callback) {
            var uri = 'http://' + localConfig.get('opentsdb:host') + ':' +
                localConfig.get('opentsdb:port') +
                '/api/uid/rename?tagv=' + oldname +
                '&name=' + newname;

            logTsdbRequest(uri);
            request.get(uri).end(function(err, res) {
                var error = err || res.error;
                if (error) {
                    log.error("Error renaming tag value ('" + oldname +
                        "'=>'" + newname + "' in OpenTSDB: " + error);
                    callback(new TsdbError(error));
                } else {
                    callback();
                }
            });
        }
    };

}());
