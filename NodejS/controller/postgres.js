/**
 * @file lib/datastore/postgres.js
 * Provides data storage and retrieval for the IPM web application in a
 * Postgres instance
 */

(function() {
    "use strict";

    /**
     * Module dependencies.
     */
    var pg = require('pg');
    var async = require('async');
    var StatsD = require('node-statsd');

    var log = require('log').namespace('postgres');

    /**
     * Lookup table of roles to IDs
     *
     * @name ROLES
     * @memberof datastore/postgres
     * @private
     */
    var USER_ROLE = {
        'Administrator': 1,
        'Testing':       2,
        'Operations':    3,
        'Residential':   4,
        'Commerical':    5,
        'UtilityStaff':  6,
        'SolarStaff':    7
    };

    /**
     * Lookup table of HPC statuses to IDs
     *
     * @name HPC_STATUS
     * @memberof datastore/postgres
     * @private
     */
    var HPC_STATUS = {
        'Assembled':            1,
        'Factory Tested':       2,
        'Factory Provisioned':  3,
        'With Wholesaler':      4,
        'With Retailer':        5,
        'Shipping to Customer': 6,
        'Inactive':             7,
        'Active':               8,
        'Retired':              9,
        'Returned Faulty':     10,
        'Certificate Revoked': 11,
        'Testing':             12
    };

    /**
     * Lookup table of Message statuses to IDs
     *
     * @name MESSAGE_STATUS
     * @memberof datastore/postgres
     * @private
     */
    var MESSAGE_STATUS = {
        'unread':   1,
        'read':     2,
        'inactive': 3
    };

    // only show 'unread' & 'read' messages
    var VALID_MESSAGE_STATUSES = Object.keys(MESSAGE_STATUS).slice(0, 2);

    /**
     * Lookup table of Message types to IDs
     *
     * @name MESSAGE_TYPE
     * @memberof datastore/postgres
     * @private
     */
    var MESSAGE_TYPE = {
        'low':                  1,
        'medium':               2,
        'high':                 3,
        'critical':             4,
        'electricity_distributor': 5,
        'tip':                  6,
        'system_support':       7,
        'user_support':         8,
        'system_marketing':     9,
        'user_marketing':       10,
        'system_status':        11,
        'home_automation':      12,
        'organisation':         13
    };

    /**
     * Connection information for our database, query, add or update only
     *
     * @name connection
     * @memberof datastore/postgres
     * @private
     */
    var connection;

    /**
     * Connection information for our database, delete only
     *
     * @name connectionDelete
     * @memberof datastore/postgres
     * @private
     */
    var connectionDelete;

    /**
     * Statsd client, to record performance statistics
     *
     * @name statsClient
     * @memberof datastore/postgres
     * @private
     */
    var statsClient = new StatsD();

    /**
     * Log pg error information.
     *
     * @name logPgError
     * @memberof datastore/postgres
     * @function
     * @private
     * @param source {String} Label describing the source of the error
     * @param err {Error} Error object from which to extract the details
     * @param caller_err {Error} Optional error object that was created
     *        prior to the call that generated err. This can be used to
     *        generate more helpful error stacks.
     */
    var logPgError = function(source, err, caller_err) {
        // Log it with an error stack.
        log.error(err, source + ' error');
        // Pass it to the logger in such a way that it doesn't interpret it
        // as an error object, and so instead records the auxillary fields
        // as JSON.
        log.error({pgerr: err}, '  pg err details');

        if (caller_err) {
            // Log the caller error stack as well.
            log.error(caller_err, '  caller');
        }
    };

    /**
     * Terminate due to an error, logging the reason on the way.
     *
     * @name terminateDueToError
     * @memberof datastore/postgres
     * @function
     * @private
     * @param reason {String} The reason for termination
     */
    var terminateDueToError = function(reason) {
        // Record failure in statsd
        statsClient.increment('postgres.failure');

        log.fatal('Terminating due to ' + reason + '...');
        process.exit(1);
    };

    /**
     * Call pg.connect with generalised error handling.
     *
     * This wrapper is useful for handling pg-specific errors that we want to
     * treat as fatal.
     *
     * @name pgConnect
     * @memberof datastore/postgres
     * @function
     * @private
     * @param connection {Object} Passed to pg.connect connection argument
     * @param callback {Function} Callback with err, client, done params,
     *                 called on completion of pg.connect
     */
    var pgConnect = function(connection, callback) {
        pg.connect(connection, function(err, client, done) {
            if (err) {
                logPgError('pg.connect', err);
                // See RAD-1559 where ECONNRESET apparently resulted in IPM not
                // recovering. This happens when the other end of a connection
                // (e.g. postgres server) is uncleanly terminated.
                // Apply the same for all connection errors, to be on the safe
                // side.
                terminateDueToError('pg connection error');
            }
            callback(err, client, done);
        });
    };

    /**
     * Call pg client.query with generalised error handling.
     *
     * This wrapper is useful for handling pg-specific errors that we want to
     * treat as fatal. This also provides more helpful logging in the case
     * of non-fatal errors.
     *
     * @name clientQuery
     * @memberof datastore/postgres
     * @function
     * @private
     * @param client {Object} pg client object from pg.connect callback
     * @param write {Boolean} Indicates whether this statement can alter data
     * @param args {Object} pg client.query arguments
     *             (typically name, text, values)
     * @param callback {Function} Callback with err, client, done params,
     *                 called on completion of pg.connect
     */
    var clientQuery = function(client, write, args, callback) {
        // Record the time taken to perform each query
        var startTime = Date.now();
        // Used to build the stats key
        var key = write ? 'write' : 'query';

        // Create an error object at this scope in case there is an error
        // during the query. This allows a more useful error stack to be
        // logged.
        var caller_err = new Error();
        client.query(args, function(err, result) {
            if (err) {
                // Record failure in statsd
                statsClient.increment('postgres.' + key + '.' + args.name + '.failure');

                // Log the error
                logPgError('pg client.query', err, caller_err);
                // Checking message/file/routine text is slightly hackish, but
                // PostgreSQL error code categories don't give the information
                // we need here (i.e. cache-specific errors).
                if (err.message.match(/cache/i) ||
                    err.file.match(/cache/i) ||
                    err.routine.match(/cache/i)) {
                    // This is for handling errors like:
                    // 'cached plan must not change result type'
                    // which happen due to prepared SQL query cache entries
                    // becoming stale when the schema is changed.
                    terminateDueToError('pg cache error');
                }
            } else {
                // Record the time to process the request in statsd.
                // NB: Date.now() is wall-clock time, and so can have
                // jumps at arbitrary times (e.g. NTP changes), and so
                // won't always give accurate elapsed time.
                statsClient.timing('postgres.' + key + '.' + args.name + '.time', Date.now() - startTime);
            }
            callback(err, result);
        });
    };

    pg.on('error', function(err) {
        logPgError('pg', err);
        // Treat any spontaneous errors (i.e. not the direct result of calls
        // to pg functions) as fatal.
        terminateDueToError('spontaneous pg error');
    });

    /**
     * @namespace Data storage and retrieval for our application
     * @name datastore
     */
    module.exports = {
        /**
         * Connect to the database.
         *
         * This really just sets up the connection pool for use later.
         *
         * @name connect
         * @memberof datastore/postgres
         * @function
         * @public
         * @param nconf {Object} The application configuration
         * @param callback {Function} Callback to fire when finished, with
         *                            error parameter
         */
        connect: function(nconf, callback) {
            var port = nconf.get('postgres:port');
            if ((process.env.POSTGRES_TEST==="local_test") && nconf.get('postgres:port_local')) {
                port = nconf.get('postgres:port_local');
            }

            connection = {
                user:     nconf.get('postgres:users:normal:username'),
                password: nconf.get('postgres:users:normal:password'),
                host:     nconf.get('postgres:hostname'),
                port:      port,
                database: nconf.get('postgres:database'),
                includes_retailer_tariff_test_data: nconf.get('testing:retailer_tariff_test_data')
            };

            connectionDelete = {
                user:     nconf.get('postgres:users:delete:username'),
                password: nconf.get('postgres:users:delete:password'),
                host:     nconf.get('postgres:hostname'),
                port:      port,
                database: nconf.get('postgres:database')
            };

            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    done();
                    pg.connect(connectionDelete, function(err, client, done) {
                        if (!err) {
                            log.info('Connected to Postgres.');
                        } else {
                            err = 'Postgres delete connection error: ' + err;
                        }
                        done();
                        callback(err);
                    });
                } else {
                    err = 'Postgres query/add/update connection error: ' + err;
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a user
         *
         * @name addOrUpdateUser
         * @memberof datastore
         * @function
         * @public
         * @param user {object} The user to save
         * @param callback {Function} Callback to fire when finished
         */
        addOrUpdateUser: function(user, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var role_id = USER_ROLE[user.role];

                    // Are we updating or inserting?
                    if (user.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateUser',
                            text:
                                'UPDATE ' +
                                '  "USER" ' +
                                'SET ' +
                                '  "USERNAME" = $1, ' +
                                '  "EMAIL" = $2, ' +
                                '  "ROLE_ID" = $3, ' +
                                '  "PASSWORD_HASH" = $4, ' +
                                '  "EMAIL_VALIDATED" = $5, ' +
                                '  "PREFERENCES" = $6, ' +
                                '  "REGISTRATION_DATA" = $7, ' +
                                '  "GIVEN_NAME" = $8, ' +
                                '  "FAMILY_NAME" = $9, ' +
                                '  "PHONE_NUMBER" = $10, ' +
                                '  "ORGANISATION_ID" = $11, ' +
                                '  "PERMISSION_ID" = $12 ' +
                                'WHERE ' +
                                '  "ID" = $13 ',
                            values: [
                                user.username,
                                user.email,
                                role_id,
                                user.password_hash,
                                user.email_validated,
                                user.preferences,
                                user.registration_data,
                                user.given_name,
                                user.family_name,
                                user.phone_number,
                                user.organisation ? user.organisation.id : null,
                                user.permission ? user.permission.id : null,
                                user.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addUser',
                            text:
                                'INSERT INTO "USER"( ' +
                                '  "USERNAME", ' +
                                '  "EMAIL", ' +
                                '  "ROLE_ID", ' +
                                '  "PASSWORD_HASH", ' +
                                '  "EMAIL_VALIDATED", ' +
                                '  "PREFERENCES", ' +
                                '  "REGISTRATION_ADDRESS_ID", ' +
                                '  "REGISTRATION_DATA", ' +
                                '  "GIVEN_NAME", ' +
                                '  "FAMILY_NAME", ' +
                                '  "PHONE_NUMBER", ' +
                                '  "ORGANISATION_ID" , ' +
                                '  "PERMISSION_ID" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                user.username,
                                user.email,
                                role_id,
                                user.password_hash,
                                user.email_validated,
                                user.preferences,
                                user.registration_address.id,
                                user.registration_data,
                                user.given_name,
                                user.family_name,
                                user.phone_number,
                                user.organisation ? user.organisation.id : null,
                                user.permission ? user.permission.id : null
                            ]
                        }, function(err, result) {
                            if (!err) {
                                user.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Save user's preferences
         *
         * @name saveUserPreferences
         * @memberof datastore
         * @function
         * @public
         * @param user {object} The user to save
         * @param callback Callback to fire when finished
         */
        saveUserPreferences: function(user, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'saveUserPreferences',
                        text:
                            'UPDATE ' +
                            '  "USER" ' +
                            'SET ' +
                            '  "PREFERENCES" = $1 ' +
                            'WHERE ' +
                            '  "ID" = $2 ',
                        values: [
                            JSON.stringify(user.preferences),
                            user.id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result && result.rowCount === 1);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a user
         *
         * @name deleteUser
         * @memberof datastore/postgres
         * @function
         * @public
         * @param user {Object} The user to delete
         * @param callback {Function} Callback to fire when finished
         */
        deleteUser: function(user, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteUser',
                        text:
                            'DELETE FROM ' +
                            '  "USER" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            user.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Test if a user is linked with the supplied HPC
         *
         * @name isUserLinkedWithHPC
         * @memberof datastore/postgres
         * @function
         * @public
         * @param user {Object} The user to check link status
         * @param hpc_id {Number} The identifier of the HPC to check link status
         * @param callback {Function} Callback with err and result parameters
         *                            The result is a boolean flag, true iff the
         *                            user is linked to the given HPC.
         *                            Note that this will give false even if
         *                            the given user doesn't exist.
         */
        isUserLinkedWithHPC: function(user, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'isUserLinkedWithHPC',
                        text:
                            'SELECT ' +
                            '  1 ' +
                            'FROM ' +
                            '  "USER_HPC" ' +
                            'WHERE ' +
                            '  "USER_ID" = $1 AND ' +
                            '  "HPC_ID" = $2 ',
                        values: [
                            user.id,
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result && result.rows && result.rows.length > 0);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Check whether a user exists and the given HPC is not linked to
         * another user.
         *
         * @name isHpcAvailableForUser
         * @memberof datastore/postgres
         * @function
         * @public
         * @param user {Object} The user to check link status
         * @param hpc_id {Number} The identifier of the HPC to check link status
         * @param callback {Function} Callback with err and result parameters
         *                            The result is a boolean flag, true iff the
         *                            user exists and the given HPC is not
         *                            linked to any other user.
         */
        isHpcAvailableForUser: function(user, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'isHpcAvailableForUser',
                        text:
                            'SELECT ' +
                            '  1' +
                            'FROM ' +
                            '  "USER"' +
                            'WHERE ' +
                            '  "ID" = $1' +
                            '  AND NOT EXISTS ( ' +
                            '    SELECT ' +
                            '      1 ' +
                            '    FROM ' +
                            '      "USER_HPC" ' +
                            '    WHERE ' +
                            '      "HPC_ID" = $2 ' +
                            '      AND "USER_ID" != $1 ' +
                            '  ) ',
                        values: [
                            user.id,
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        var available = result && result.rows && (result.rows.length > 0);
                        callback(err, available);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Link a user with an HPC, if the HPC is not already linked to any
         * user.
         *
         * @name linkUserWithHPC
         * @memberof datastore/postgres
         * @function
         * @public
         * @param user {Object} The user to link with HPC
         * @param hpc_id {Number} The identifier of the HPC to link with user
         * @param callback {Function} Callback to fire when finished with
         *                            err result parameters
         *                            The result parameter is boolean true iff
         *                            the link was made (i.e. the hpc_id was
         *                            not already linked to any user).
         */
        linkUserWithHPC: function(user, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'linkUserWithHPC',
                        text:
                            'INSERT INTO "USER_HPC"( ' +
                            '  "USER_ID", ' +
                            '  "HPC_ID" ' +
                            ') ' +
                            'SELECT ' +
                            '  $1, ' +
                            '  $2 ' +
                            'WHERE ' +
                            '  NOT EXISTS ( ' +
                            '    SELECT ' +
                            '      1 ' +
                            '    FROM ' +
                            '      "USER_HPC" ' +
                            '    WHERE ' +
                            '      "HPC_ID" = $2 ' +
                            '  ) ',
                        values: [
                            user.id,
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result && result.rowCount === 1);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delink a user from a supplied HPC
         *
         * @name delinkUserFromHPC
         * @memberof datastore/postgres
         * @function
         * @public
         * @param user {Object} The user to delink
         * @param hpc_id {Number} The identifier of the HPC to delink
         * @param callback Callback to fire when finished
         */
        delinkUserFromHPC: function(user, hpc_id, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'delinkUserFromHPC',
                        text:
                            'DELETE FROM ' +
                            '  "USER_HPC" ' +
                            'WHERE ' +
                            '  "USER_ID" = $1 AND ' +
                            '  "HPC_ID" = $2 ',
                        values: [
                            user.id,
                            hpc_id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find Users who have not validated their email
         *
         * @name findUsersWithEmailNotValidated
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUsersWithEmailNotValidated: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUsersWithEmailNotValidated',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_USER" ' +
                            'WHERE ' +
                            '  "EMAIL_VALIDATED" is NULL ',
                        values: []
                    }, function(err, result) {
                        var users = result ? result.rows : null;
                        done();
                        callback(err, users);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find all users
         *
         * @name findUsersAll
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result
         * parameters
         */
        findUsersAll: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUsersAll',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_USER" ',
                        values: []
                    }, function(err, result) {
                        var users = result ? result.rows : null;
                        done();
                        callback(err, users);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a user with the supplied email address
         *
         * @name findUserByEmail
         * @memberof datastore
         * @function
         * @public
         * @param email {String} The email of the desired user
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUserByEmail: function(email, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUserByEmail',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_USER" ' +
                            'WHERE ' +
                            '  lower("EMAIL") = lower($1) ',
                        values: [
                            email
                        ]
                    }, function(err, result) {
                        done();

                        var user = result && result.rows ? result.rows[0] : null;
                        callback(err, user);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the user that installed a mobile app.
         * The app should have 'Enabled' status
         *
         * @name findUserByActiveAppInstall
         * @memberof datastore
         * @function
         * @public
         * @param install_id {String} The installation identifier
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUserByActiveAppInstall: function(install_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUserByActiveAppInstall',
                        text:
                            'SELECT ' +
                            '  "V_USER".* ' +
                            'FROM ' +
                            '  "V_USER", ' +
                            '  "APP_INSTALL" ' +
                            'WHERE ' +
                            '  "V_USER"."ID" = "APP_INSTALL"."USER_ID" AND ' +
                            '  "APP_INSTALL"."STATUS_ID" = 1 AND ' +
                            '  "APP_INSTALL"."EXTERNAL_INSTALL_ID" = $1 ',
                        values: [
                            install_id
                        ]
                    }, function(err, result) {
                        done();

                        var user = result && result.rows ? result.rows[0] : null;
                        callback(err, user);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all HPCs that are accessible by the supplied username
         *
         * @name findHpcsOwnedByUsername
         * @memberof datastore
         * @function
         * @public
         * @param username {String} The username of the owner
         * @param limit {Number} The maximum number of records to return
         * @param offset {Number} The start of the result set
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcsOwnedByUsername: function(username, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_HPC".* ' +
                        'FROM ' +
                        '  "V_HPC" ' +
                        '  INNER JOIN "USER_HPC" ON "V_HPC"."ID" = "USER_HPC"."HPC_ID" ' +
                        '  INNER JOIN "USER" ON "USER_HPC"."USER_ID" = "USER"."ID" ' +
                        'WHERE ' +
                        '  "USER"."USERNAME" = $1 ' +
                        'ORDER BY ' +
                        '  "V_HPC"."CREATED" DESC ';
                    var params = [
                        username
                    ];

                    if (limit) {
                        sql +=
                            'OFFSET ' +
                            '  $2 ' +
                            'LIMIT ' +
                            '  $3 ';
                        params.push(offset, limit);
                    }
                    clientQuery(client, false, {
                        name: 'findHpcsOwnedByUsername' + (limit ? '_withLimit' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        var hpcs = result ? result.rows : null;
                        done();

                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all HPCs that are accessible by the supplied username that
         * has a paired meter.
         *
         * @name findHpcsOwnedByUsernameWithPairedMeter
         * @memberof datastore
         * @function
         * @public
         * @param username {String} The username of the owner
         * @param limit {Number} The maximum number of records to return
         * @param offset {Number} The start of the result set
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcsOwnedByUsernameWithPairedMeter: function(username, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_HPC".* ' +
                        'FROM ' +
                        '  "V_HPC" ' +
                        '  INNER JOIN "USER_HPC" ON "V_HPC"."ID" = "USER_HPC"."HPC_ID" ' +
                        '  INNER JOIN "USER" ON "USER_HPC"."USER_ID" = "USER"."ID" ' +
                        'WHERE ' +
                        '  "USER"."USERNAME" = $1 ' +
                        '  AND "V_HPC"."SMART_METER_FIRST_PAIRED" IS NOT NULL ' +
                        'ORDER BY ' +
                        '  "V_HPC"."CREATED" DESC ';
                    var params = [
                        username
                    ];

                    if (limit) {
                        sql +=
                            'OFFSET ' +
                            '  $2 ' +
                            'LIMIT ' +
                            '  $3 ';
                        params.push(offset, limit);
                    }
                    clientQuery(client, false, {
                        name: 'findHpcsOwnedByUsernameWithPairedMeter' + (limit ? '_withLimit' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        var hpcs = result ? result.rows : null;
                        done();

                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all available HPCs.
         * Caution: for internal use only (e.g. cron jobs)
         *
         * @name findAllHpcs
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAllHpcs: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAllHpcs',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HPC" ',
                        values: []
                    }, function(err, result) {
                        var hpcs = result ? result.rows : null;
                        done();
                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for eligible HPCs for the given analytics filters
         *
         * Note: All filters are optional. But even with no filters given, this
         * is still not identical to findAll(). HPCs must meet these basic
         * criteria to be eligible:
         * a. Paired with a meter. We check on smart_meter.first_paired.
         * b. Linked to a user account. This implies that all billing details
         *    and address information checks are done against HPC instead of the
         *    temporary user.registration_data
         *
         * @name findHpcsForAnalytics
         * @memberof datastore.postgres
         * @function
         * @public
         * @param filters {Object}
         *   The filters, at least a dummy object. Currently supports (all
         *   optional):
         * @param filters.country {String} The country as used in address
         * @param filters.state {String} The state as used in address
         * @param filters.postcode {String} The postcode as used in address
         * @param filters.distributor {String}
         *   The distributor as specified in billing details
         * @param filters.retailer {String}
         *   The retailer as specified in billing details
         * @param filters.solar {Boolean} Whether the hpc has a solar tariff
         * @param filters.rego-start {Date}
         *   The HPC's primary user's registration date range's inclusive lower
         *   bound. Open lower bound if not provided.
         * @param filters.rego-end {Date}
         *   The HPC's primary user's registration date range's inclusive upper
         *   bound. Open upper bound if not provided.
         * @param filters.organisation {String}
         *   The HPC's primary user's organisation
         * @param callback {Function}
         *   A callback with the usual error and result parameters
         */
        findHpcsForAnalytics: function(filters, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var statement = {
                        name: 'findHpcsForAnalytics',
                        text:
                            // The TARIFF queries try to pull out a first
                            // valid tariff for the given hpc and user to check
                            // if the user/hpc pair is a solar one.
                            // We try to use the same logic as the route of
                            // /admin/user page (referred to as 'that page'
                            // below). A few notes here:
                            // a. That page uses findTariffForHPCForUserAtTime
                            //    for given HPC/User. We created a LEFT JOIN
                            //    on TARIFF with almost exact conditions as that
                            //    page's WHERE clause in our ON condition.
                            // b. That page passed a 'now' JS timestamp to SQL.
                            //    We use a hardcoded NOW() to simplify the
                            //    parameter passing from upstream.
                            // c. In the callback of clientQuery in that
                            //    findTariffForHPCForUserAtTime, only the first
                            //    valid tariff is passed back to upstream after
                            //    sorted by CREATED_BY_USER_ID. We mimic that
                            //    behavior by using the DISTINCT ON with
                            //    CREATED_BY_USER_ID and V_HPC.ID with ORDER BY
                            //    on the two fields.
                            'SELECT ' +
                            '  DISTINCT ON("TARIFF"."CREATED_BY_USER_ID", "V_HPC"."ID", "V_USER"."ID") "TARIFF"."ID" AS "TARIFF_ID", ' +
                            '  "TARIFF"."COMPLEX_TARIFF_DATA" AS "TARIFF_COMPLEX_TARIFF_DATA", ' +
                            '  "V_HPC".* ' +
                            'FROM ' +
                            '  "V_HPC" ' +
                            '    LEFT JOIN "USER_HPC" ON (("V_HPC"."ID" = "USER_HPC"."HPC_ID")) ' +
                            '    LEFT JOIN "V_USER" ON (("USER_HPC"."USER_ID" = "V_USER"."ID")) ' +
                            '    LEFT JOIN "TARIFF" ON (( ' +
                            '      (  ("USER_HPC"."HPC_ID" = "TARIFF"."HPC_ID" AND ("TARIFF"."CREATED_BY_USER_ID" IS NULL OR "TARIFF"."CREATED_BY_USER_ID" = "USER_HPC"."USER_ID")) ' +
                            '         OR ' +
                            '         ("USER_HPC"."USER_ID" = "TARIFF"."CREATED_BY_USER_ID" AND "TARIFF"."HPC_ID" IS NULL) ' +
                            '      ) ' +
                            '      AND ' +
                            '      (  "TARIFF"."VALID_FROM" <= NOW() AND ("TARIFF"."VALID_UNTIL" IS NULL OR "TARIFF"."VALID_UNTIL" > NOW()) ) ' +
                            '    )) ' +
                            'WHERE ',
                        values: []
                    };

                    var where_clauses = ['"V_HPC"."SMART_METER_FIRST_PAIRED" IS NOT NULL'];

                    [{
                        filter_name: 'country',
                        short: 'Cn',
                        column_name: 'ADDRESS_COUNTRY'
                    }, {
                        filter_name: 'state',
                        short: 'St',
                        column_name: 'ADDRESS_STATE'
                    }, {
                        filter_name: 'postcode',
                        short: 'Pc',
                        column_name: 'ADDRESS_POSTCODE'
                    }, {
                        filter_name: 'distributor',
                        short: 'Ds',
                        column_name: 'ELECTRICITY_DISTRIBUTOR'
                    }, {
                        filter_name: 'retailer',
                        short: 'Rt',
                        column_name: 'ELECTRICITY_RETAILER'
                    }].forEach(function(item) {
                        var filter_value = filters[item.filter_name];
                        if (filter_value) {
                            statement.name += item.short;
                            statement.values.push(filter_value);
                            where_clauses.push('"V_HPC"."SMART_METER_BILLING_DETAILS_' + item.column_name + '" = $' + statement.values.length);
                        }
                    });

                    if (filters.solar !== undefined) {
                        statement.name += 'So' + (filters.solar ? 't' : 'f');
                        // mimic the following JS check in views/admin/user.jade
                        //   results.tariff && results.tariff.complex_tariff_data &&
                        //       results.tariff.complex_tariff_data.solar &&
                        //       results.tariff.complex_tariff_data.solar.type !== 'none'
                        where_clauses.push('(' +
                            '("TARIFF"."COMPLEX_TARIFF_DATA"::json->\'solar\') IS ' + (filters.solar ? 'NOT ' : '') + ' NULL ' +
                            (filters.solar ? 'AND ' : 'OR ') +
                            '("TARIFF"."COMPLEX_TARIFF_DATA"::json#>>\'{solar,type}\') ' + (filters.solar ? '<>' : '=') + ' \'none\'' +
                        ')');
                    }

                    if (filters['rego-start']) {
                        statement.name += 'Rs';
                        statement.values.push(filters['rego-start']);
                        where_clauses.push('"V_USER"."CREATED" >= $' + statement.values.length);
                    }
                    if (filters['rego-end']) {
                        statement.name += 'Re';
                        statement.values.push(filters['rego-end']);
                        where_clauses.push('"V_USER"."CREATED" <= $' + statement.values.length);
                    }

                    if (filters.organisation) {
                        statement.name += 'Og';
                        statement.values.push(filters.organisation);
                        where_clauses.push('"V_USER"."ORGANISATION_NAME" = $' + statement.values.length);
                    }

                    statement.text += where_clauses.join(' AND ');
                    statement.text += ' ORDER BY "TARIFF"."CREATED_BY_USER_ID", "V_HPC"."ID" ';

                    clientQuery(client, false, statement, function(err, result) {
                        var hpcs = result ? result.rows : null;
                        done();
                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Store HPC settings
         *
         * @name saveHpcSettings
         * @memberof datastore
         * @function
         * @public
         * @param hpc {Object} The HPC object
         * @param callback Callback to fire when finished
         */
        saveHpcSettings: function(hpc, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'saveHpcSettings',
                        text:
                            'UPDATE ' +
                            '  "HPC" ' +
                            'SET ' +
                            '  "SETTINGS" = $1 ' +
                            'WHERE ' +
                            '  "ID" = $2 ',
                        values: [
                            JSON.stringify(hpc.settings),
                            hpc.id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result && result.rowCount === 1);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Store HPC status
         *
         * @name saveHpcStatus
         * @memberof datastore
         * @function
         * @public
         * @param hpc {Object} The HPC object
         * @param callback Callback to fire when finished
         */
        saveHpcStatus: function(hpc, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var status_id = HPC_STATUS[hpc.status];
                    clientQuery(client, true, {
                        name: 'saveHpcStatus',
                        text:
                        'UPDATE ' +
                        '  "HPC" ' +
                        'SET ' +
                        '  "HPC_STATUS_ID" = $1 ' +
                        'WHERE ' +
                        '  "ID" = $2 ',
                        values: [
                            status_id,
                            hpc.id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result && result.rowCount === 1);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },


        /**
         * Search for all users that owning the HPC by its serial
         *
         * @name findUsersOwningHpcSerial
         * @memberof datastore
         * @function
         * @public
         * @param serial {String} The serial of the HPC
         * @param limit {Number} The maximum number of records to return
         * @param offset {Number} The start of the result set
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUsersOwningHpcSerial: function(serial, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_USER".* ' +
                        'FROM ' +
                        '  "V_USER" ' +
                        '  INNER JOIN "USER_HPC" ON "V_USER"."ID" = "USER_HPC"."USER_ID" ' +
                        '  INNER JOIN "HPC" ON "USER_HPC"."HPC_ID" = "HPC"."ID" ' +
                        'WHERE ' +
                        '  "HPC"."SERIAL" = $1 ' +
                        'ORDER BY ' +
                        '  "V_USER"."CREATED" DESC ';
                    var params = [
                        serial
                    ];

                    if (limit) {
                        sql +=
                            'OFFSET ' +
                            '  $2 ' +
                            'LIMIT ' +
                            '  $3 ';
                        params.push(offset, limit);
                    }
                    clientQuery(client, false, {
                        name: 'findUsersOwningHpcSerial' + (limit ? '_withLimit' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        var users = result ? result.rows : null;
                        done();

                        callback(err, users);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the primary user owning the HPC, by its private id.
         *
         * @name findPrimaryUserForHpcID
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {String} The private identifier of the HPC
         * @param callback {Function} A callback with error and result params
         *                            The result is a user DTO or null if none
         *                            found.
         */
        findPrimaryUserForHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_USER".* ' +
                        'FROM ' +
                        '  "V_USER" ' +
                        '  INNER JOIN "USER_HPC" ON "V_USER"."ID" = "USER_HPC"."USER_ID" ' +
                        'WHERE ' +
                        '  "USER_HPC"."HPC_ID" = $1 ' +
                        'ORDER BY ' +
                        '  "V_USER"."CREATED" DESC ' +
                        'LIMIT 1';
                    var params = [
                        hpc_id
                    ];
                    clientQuery(client, false, {
                        name: 'findPrimaryUserForHpcID',
                        text: sql,
                        values: params
                    }, function(err, result) {
                        var user = (result && result.rows && result.rows[0]) || null;
                        done();

                        callback(err, user);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a user with the supplied username
         *
         * @name findUserByUsername
         * @memberof datastore
         * @function
         * @public
         * @param username {String} The name of the desired user
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUserByUsername: function(username, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUserByUsername',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_USER" ' +
                            'WHERE ' +
                            '  lower("USERNAME") = lower($1) ',
                        values: [
                            username
                        ]
                    }, function(err, result) {
                        done();

                        var user = result && result.rows ? result.rows[0] : null;
                        callback(err, user);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a user with the supplied username or email address
         *
         * @name findUserByUsernameOrEmail
         * @memberof datastore
         * @function
         * @public
         * @param username {String} The name of the desired user
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findUserByUsernameOrEmail: function(username, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findUserByUsernameOrEmail',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_USER" ' +
                            'WHERE ' +
                            '  lower("USERNAME") = lower($1) OR ' +
                            '  lower("EMAIL") = lower($2) ',
                        values: [
                            username,
                            username
                        ]
                    }, function(err, result) {
                        done();

                        var user = result && result.rows ? result.rows[0] : null;
                        callback(err, user);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for eligible hpcless users for the given filters
         *
         * Note:
         * a. All filters are optional.
         * b. This looks very similar to findHpcsForAnalytics. But it does not
         *    join the HPC table. That means anything we search must be
         *    available thru USER and TARIFF tables.
         *
         * @name findHpclessUsersWithFilters
         * @memberof datastore.postgres
         * @function
         * @public
         * @param filters {Object}
         *   The filters, at least a dummy object. Currently supports (all
         *   optional):
         * @param filters.country {String} The country as used in registration address
         * @param filters.state {String} The state as used in registration address
         * @param filters.postcode {String} The postcode as used in registration address
         * @param filters.distributor {String} The distributor as specified in billing details
         * @param filters.retailer {String} The retailer as specified in billing details
         * @param filters.solar {Boolean} Whether the user has created a solar tariff
         * @param filters.rego-start {Date}
         *   The user's registration date range's inclusive lower bound. Open lower bound if skipped
         * @param filters.rego-end {Date}
         *   The user's registration date range's inclusive upper bound. Open upper bound if skipped
         * @param filters.organisation {String} The user's organisation
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpclessUsersWithFilters: function(filters, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var statement = {
                        name: 'findUsersWithFilters',
                        text:
                            // Similar mimic of findTariffForHPCForUserAtTime as
                            // in findHpcsForAnalytics without joining HPC table
                            'SELECT ' +
                            ' DISTINCT ON("TARIFF"."CREATED_BY_USER_ID", "TARIFF"."HPC_ID", "V_USER"."ID") "TARIFF"."ID" AS "TARIFF_ID", ' +
                            '  "V_USER".* ' +
                            'FROM ' +
                            '  "V_USER" ' +
                            '    LEFT JOIN "TARIFF" ON ( ' +
                            '      (  "V_USER"."ID" = "TARIFF"."CREATED_BY_USER_ID" AND "TARIFF"."HPC_ID" IS NULL ) ' +
                            '      AND ' +
                            '      (  "TARIFF"."VALID_FROM" <= NOW() AND ("TARIFF"."VALID_UNTIL" IS NULL OR "TARIFF"."VALID_UNTIL" > NOW()) ) ' +
                            '    ) ' +
                            'WHERE ',
                        values: []
                    };

                    var where_clauses = ['"V_USER"."ID" NOT IN ( SELECT DISTINCT "USER_ID" FROM "USER_HPC" )'];

                    [{
                        filter_name: 'country',
                        short: 'Cn'
                    }, {
                        filter_name: 'state',
                        short: 'St'
                    }, {
                        filter_name: 'postcode',
                        short: 'Pc'
                    }].forEach(function(item) {
                        var filter_value = filters[item.filter_name];
                        if (filter_value) {
                            statement.name += item.short;
                            statement.values.push(filter_value);
                            where_clauses.push('"V_USER"."REGISTRATION_ADDRESS_' + item.filter_name.toUpperCase() + '" = $' + statement.values.length);
                        }
                    });

                    [{
                        filter_name: 'distributor',
                        short: 'Ds'
                    }, {
                        filter_name: 'retailer',
                        short: 'Rt'
                    }].forEach(function(item) {
                        var filter_value = filters[item.filter_name];
                        if (filter_value) {
                            statement.name += item.short;
                            statement.values.push(filter_value);
                            where_clauses.push('("V_USER"."REGISTRATION_DATA"::json->>\'' + item.filter_name + '\') = $' + statement.values.length);
                        }
                    });

                    if (filters.solar !== undefined) {
                        statement.name += 'So' + (filters.solar ? 't' : 'f');
                        // mimic the following JS check in views/admin/user.jade
                        //   results.tariff && results.tariff.complex_tariff_data &&
                        //       results.tariff.complex_tariff_data.solar &&
                        //       results.tariff.complex_tariff_data.solar.type !== 'none'
                        where_clauses.push('(' +
                            '("TARIFF"."COMPLEX_TARIFF_DATA"::json->\'solar\') IS ' + (filters.solar ? 'NOT ' : '') + ' NULL ' +
                            (filters.solar ? 'AND ' : 'OR ') +
                            '("TARIFF"."COMPLEX_TARIFF_DATA"::json#>>\'{solar,type}\') ' + (filters.solar ? '<>' : '=') + ' \'none\'' +
                        ')');
                    }

                    if (filters['rego-start']) {
                        statement.name += 'Rs';
                        statement.values.push(filters['rego-start']);
                        where_clauses.push('"V_USER"."CREATED" >= $' + statement.values.length);
                    }
                    if (filters['rego-end']) {
                        statement.name += 'Re';
                        statement.values.push(filters['rego-end']);
                        where_clauses.push('"V_USER"."CREATED" <= $' + statement.values.length);
                    }

                    if (filters.organisation) {
                        statement.name += 'Og';
                        statement.values.push(filters.organisation);
                        where_clauses.push('"V_USER"."ORGANISATION_NAME" = $' + statement.values.length);
                    }

                    statement.text += where_clauses.join(' AND ');
                    statement.text += ' ORDER BY "TARIFF"."CREATED_BY_USER_ID" ';

                    clientQuery(client, false, statement, function(err, result) {
                        var hpcs = result ? result.rows : null;
                        done();
                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update an HPC
         *
         * @name addOrUpdateHpc
         * @memberof datastore
         * @function
         * @public
         * @param hpc {object} The HPC to save
         * @param callback {Function} Callback to fire when finished
         */
        addOrUpdateHpc: function(hpc, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var status_id = HPC_STATUS[hpc.status];
                    // Are we updating or inserting?
                    if (hpc.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHpc',
                            text:
                                'UPDATE ' +
                                '  "HPC" ' +
                                'SET ' +
                                '  "SERIAL" = $1, ' +
                                '  "HPC_STATUS_ID" = $2, ' +
                                '  "DEVICE_ID" = $3, ' +
                                '  "MAC_ADDRESS" = $4, ' +
                                '  "INSTALL_CODE" = $5, ' +
                                '  "SMART_METER_ID" = $6, ' +
                                '  "SETTINGS" = $7, ' +
                                '  "SELF_TEST" = $8, ' +
                                '  "DISAGGREGATE" = $9 ' +
                                'WHERE ' +
                                '  "ID" = $10 ',
                            values: [
                                hpc.serial,
                                status_id,
                                hpc.device.id,
                                hpc.mac_address,
                                hpc.install_code,
                                hpc.smart_meter.id,
                                hpc.settings,
                                hpc.self_test || {},
                                hpc.disaggregate,
                                hpc.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHpc',
                            text:
                                'INSERT INTO "HPC"( ' +
                                '  "SERIAL", ' +
                                '  "HPC_STATUS_ID", ' +
                                '  "DEVICE_ID", ' +
                                '  "CREATED", ' +
                                '  "MAC_ADDRESS", ' +
                                '  "INSTALL_CODE", ' +
                                '  "SMART_METER_ID", ' +
                                '  "SETTINGS", ' +
                                '  "SELF_TEST", ' +
                                '  "DISAGGREGATE" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                hpc.serial,
                                status_id,
                                hpc.device.id,
                                hpc.created,
                                hpc.mac_address,
                                hpc.install_code,
                                hpc.smart_meter.id,
                                hpc.settings,
                                hpc.self_test || {},
                                hpc.disaggregate || false
                            ]
                        }, function(err, result) {
                            if (!err) {
                                hpc.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for an HPC with the supplied private identifier
         *
         * @name findHpcByInternalId
         * @memberof datastore
         * @function
         * @public
         * @param id {String} The internal identifier of the desired HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcByInternalId: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcByInternalId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HPC" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();

                        var hpc = result && result.rows ? result.rows[0] : null;
                        callback(err, hpc);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for an HPC with the supplied public identifier
         *
         * @name findHpcBySerial
         * @memberof datastore
         * @function
         * @public
         * @param serial {String} The public serial identifier of the desired HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcBySerial: function(serial, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcBySerial',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HPC" ' +
                            'WHERE ' +
                            '  "SERIAL" = $1 ',
                        values: [
                            serial
                        ]
                    }, function(err, result) {
                        done();

                        var hpc = result && result.rows ? result.rows[0] : null;
                        callback(err, hpc);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for HPCs owned by a user that is a member of the supplied organisation.
         *
         * @name findByOrganisation
         * @memberof datastore
         * @function
         * @public
         * @param organisation {String} The organisation name
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcsByUserOrganisation: function(organisation, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcsByUserOrganisation',
                        text:
                            'SELECT ' +
                            '  "V_HPC".* ' +
                            'FROM ' +
                            '  "V_HPC" ' +
                            '  INNER JOIN "USER_HPC" ON "V_HPC"."ID" = "USER_HPC"."HPC_ID" ' +
                            '  INNER JOIN "USER" ON "USER_HPC"."USER_ID" = "USER"."ID" ' +
                            '  INNER JOIN "ORGANISATION" ON "USER"."ORGANISATION_ID" = "ORGANISATION"."ID" ' +
                            'WHERE ' +
                            '  "ORGANISATION"."NAME" = $1 ' +
                            'ORDER BY ' +
                            '  "V_HPC"."CREATED" DESC ',
                        values: [
                            organisation
                        ]
                    }, function(err, result) {
                        done();

                        var hpcs = result && result.rows ? result.rows : null;
                        callback(err, hpcs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },


        /**
         * Insert/update a Message
         *
         * @name addOrUpdateMessage
         * @memberof datastore
         * @function
         * @public
         * @param message {object} The Message to save
         * @param callback {Function} Callback to fire when finished
         */
        addOrUpdateMessage: function(message, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var status_id = MESSAGE_STATUS[message.status];

                    // Are we updating or inserting?
                    if (message.id) {
                        // It's an update
                        // all mutable fields of a message are:
                        // FROM_USER_ID, when sender anonymised
                        // MESSAGE_STATUS_ID, when status changed
                        // READ, when status changed
                        // REQUIRE_ACK, when ack command processed?
                        var statement = {
                            name: 'updateMessage'
                        };
                        if (message.from) {
                            // subquery user table to fill the from_user_id
                            statement.name += '_withFrom';
                            statement.text =
                                'UPDATE ' +
                                '  "MESSAGE" ' +
                                'SET ' +
                                '  "FROM_USER_ID" = "FROM_USER"."ID", ' +
                                '  "MESSAGE_STATUS_ID" = $1, ' +
                                '  "READ" = $2, ' +
                                '  "REQUIRE_ACK" = $3 ' +
                                'FROM ( ' +
                                '  SELECT ' +
                                '    "ID" ' +
                                '  FROM ' +
                                '    "USER" ' +
                                '  WHERE ' +
                                '    "USERNAME" = $4 ' +
                                ') AS "FROM_USER" ' +
                                'WHERE ' +
                                '  "MESSAGE"."ID" = $5 ';
                            statement.values = [
                                status_id,
                                message.read,
                                message.require_ack,
                                message.from,
                                message.id
                            ];
                        } else {
                            statement.text =
                                'UPDATE ' +
                                '  "MESSAGE" ' +
                                'SET ' +
                                '  "FROM_USER_ID" = DEFAULT, ' +
                                '  "MESSAGE_STATUS_ID" = $1, ' +
                                '  "READ" = $2, ' +
                                '  "REQUIRE_ACK" = $3 ' +
                                'WHERE ' +
                                '  "ID" = $4 ';
                            statement.values = [
                                status_id,
                                message.read,
                                message.require_ack,
                                message.id
                            ];
                        }
                        clientQuery(client, true, statement, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        var type_id = MESSAGE_TYPE[message.type];
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addMessage',
                            text:
                                'INSERT INTO "MESSAGE"( ' +
                                '  "TO_USER_ID", ' +
                                '  "FROM_USER_ID", ' +
                                '  "FROM_HPC_ID", ' +
                                '  "ORGANISATION_ID", ' +
                                '  "CREATED", ' +
                                '  "EXTERNAL_MESSAGE_ID", ' +
                                '  "REQUIRE_ACK", ' +
                                '  "READ", ' +
                                '  "MIME_TYPE", ' +
                                '  "CONTENT", ' +
                                '  "EXPIRED", ' +
                                '  "STARTED", ' +
                                '  "MESSAGE_STATUS_ID", ' +
                                '  "MESSAGE_TYPE_ID" ' +
                                ') ' +
                                'SELECT ' +
                                '  (SELECT "ID" FROM "USER" WHERE "USERNAME" = $1), ' +
                                '  (SELECT "ID" FROM "USER" WHERE "USERNAME" = $2), ' +
                                '  (SELECT "ID" FROM "HPC" WHERE "SERIAL" = $3), ' +
                                '  (SELECT "ORGANISATION_ID" FROM "USER" WHERE "USERNAME" = $1), ' +
                                '  $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                message.to,
                                message.from,
                                message.hpc,
                                message.created,
                                message.external_message_id,
                                message.require_ack,
                                message.read,
                                message.mime_type,
                                message.content,
                                message.expired,
                                message.started,
                                status_id,
                                type_id
                            ]
                        }, function(err, result) {
                            if (!err) {
                                message.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a message
         *
         * @name deleteMessage
         * @memberof datastore/postgres
         * @function
         * @public
         * @param message {Object} The message to delte
         * @param callback {Function} Callback to fire when finished
         */
        deleteMessage: function(message, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteMessage',
                        text:
                            'DELETE FROM ' +
                            '  "MESSAGE" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            message.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a message with the supplied id
         *
         * @name findMessageById
         * @memberof datastore
         * @function
         * @public
         * @param id {Number} The message id
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findMessageById: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_MESSAGE".* ' +
                        'FROM ' +
                        '  "V_MESSAGE" ' +
                        'WHERE ' +
                        '  "V_MESSAGE"."ID" = $1 ';
                    var params = [id];
                    clientQuery(client, false, {
                        name: 'findMessageById',
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();
                        var message = result && result.rows ? result.rows[0] : null;
                        callback(err, message);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for Messages with the supplied external message id & receiver username
         *
         * @name findMessagesByExternalId
         * @memberof datastore
         * @function
         * @public
         * @param to {String} The username of the receiver
         * @param hpc {String} The beatbox serial
         * @param externalId {Number} The external identifier of the message
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findMessagesByExternalId: function(to, hpc, externalId, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_MESSAGE".* ' +
                        'FROM ' +
                        '  "V_MESSAGE" ' +
                        'WHERE ' +
                        '  "V_MESSAGE"."TO_USER_USERNAME" = $1 AND ' +
                        '  "V_MESSAGE"."FROM_HPC_SERIAL" = $2 AND ' +
                        '  "V_MESSAGE"."EXTERNAL_MESSAGE_ID" = $3 ';
                    var params = [to, hpc, externalId];
                    clientQuery(client, false, {
                        name: 'findMessagesByExternalId',
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();
                        var messages = result ? result.rows : null;
                        callback(err, messages);
                    });
                } else {
                    done();
                    callback(err, null);
                }
            });
        },

        /**
         * Search for messages with the supplied receiver username
         * Note this function always brings pagination related limit/offset
         * params. Use the findMessagesCountByReceiver for the total numbers
         *
         * @name findMessagesByReceiver
         * @memberof datastore
         * @function
         * @public
         * @param to {String} The username of the receiver
         * @param type {String} An option to restrict the type of messages returned
         * @param limit {Number} The maximum number of records to return
         * @param offset {Number} The start of the result set
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findMessagesByReceiver: function(to, type, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_MESSAGE".* ' +
                        'FROM ' +
                        '  "V_MESSAGE" ' +
                        'WHERE ' +
                        '  "V_MESSAGE"."TO_USER_USERNAME" = $1 AND ' +
                        '  "V_MESSAGE"."STARTED" <= NOW() AND ' +
                        '  ("V_MESSAGE"."EXPIRED" IS NULL OR "V_MESSAGE"."EXPIRED" > NOW()) AND ' +
                        '  "V_MESSAGE"."MESSAGE_STATUS_NAME" IN ($2, $3) ';

                    var params = [
                        to,
                        VALID_MESSAGE_STATUSES[0],
                        VALID_MESSAGE_STATUSES[1]
                    ];

                    if (type) {
                        sql +=
                            '  AND "V_MESSAGE"."MESSAGE_TYPE_NAME" = $4 ' +
                            'ORDER BY ' +
                            '  "V_MESSAGE"."STARTED" DESC, ' +
                            '  "V_MESSAGE"."EXPIRED" DESC ' +
                            'OFFSET ' +
                            '  $5 ' +
                            'LIMIT ' +
                            '  $6 ';
                        params.push(type);
                    } else {
                        sql +=
                            'ORDER BY ' +
                            '  "V_MESSAGE"."STARTED" DESC, ' +
                            '  "V_MESSAGE"."EXPIRED" DESC ' +
                            'OFFSET ' +
                            '  $4 ' +
                            'LIMIT ' +
                            '  $5 ';
                    }
                    params.push(offset, limit);

                    clientQuery(client, false, {
                        name: 'findMessagesByReceiver' + (type ? '_withType' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();
                        var messages = result ? result.rows : null;
                        callback(err, messages);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for total number of messages with the supplied receiver username
         *
         * @name findMessagesCountByReceiver
         * @memberof datastore
         * @function
         * @public
         * @param to {String} The username of the receiver
         * @param type {String} An option to restrict the type of messages returned
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findMessagesCountByReceiver: function(to, type, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "V_MESSAGE"."MESSAGE_STATUS_NAME" AS "STATUS", ' +
                        '  COUNT("V_MESSAGE".*) AS "COUNT" ' +
                        'FROM ' +
                        '  "V_MESSAGE" ' +
                        'WHERE ' +
                        '  "V_MESSAGE"."TO_USER_USERNAME" = $1 AND ' +
                        '  "V_MESSAGE"."STARTED" <= NOW() AND ' +
                        '  ("V_MESSAGE"."EXPIRED" IS NULL OR "V_MESSAGE"."EXPIRED" > NOW()) AND ' +
                        '  "V_MESSAGE"."MESSAGE_STATUS_NAME" IN ($2, $3) ';

                    var params = [
                        to,
                        VALID_MESSAGE_STATUSES[0],
                        VALID_MESSAGE_STATUSES[1]
                    ];

                    if (type) {
                        sql +=
                            '  AND "V_MESSAGE"."MESSAGE_TYPE_NAME" = $4 ';
                        params.push(type);
                    }

                    sql +=
                        'GROUP BY ' +
                        '  "V_MESSAGE"."MESSAGE_STATUS_NAME" ';
                    clientQuery(client, false, {
                        name: 'findMessagesCountByReceiver' + (type ? '_withType' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();

                        var retval = {
                            total:  0,
                            unread: 0
                        };

                        if (!err && result && result.rows) {
                            result.rows.forEach(function(row) {
                                retval.total += parseInt(row.COUNT, 10);
                                if (row.STATUS === 'unread') {
                                    retval.unread = parseInt(row.COUNT, 10);
                                }
                            });
                        }

                        callback(err, retval);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all the messages sent to the supplied user without
         * limiting status and type
         *
         * @name findRawMessagesByReceiver
         * @memberof datastore.postgres
         * @function
         * @public
         * @param to {String} The username
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRawMessagesByReceiver: function(to, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findRawMessagesByReceiver',
                        text:
                            'SELECT ' +
                            '  "V_MESSAGE".* ' +
                            'FROM ' +
                            '  "V_MESSAGE" ' +
                            'WHERE ' +
                            '  "V_MESSAGE"."TO_USER_USERNAME" = $1 ',
                        values: [
                            to
                        ]
                    }, function(err, result) {
                        done();
                        var messages = result ? result.rows : null;
                        callback(err, messages);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all the messages sent from the supplied user without
         * limiting status and type
         *
         * @name findRawMessagesBySender
         * @memberof datastore.postgres
         * @function
         * @public
         * @param from {String} The username
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRawMessagesBySender: function(from, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findRawMessagesBySender',
                        text:
                            'SELECT ' +
                            '  "V_MESSAGE".* ' +
                            'FROM ' +
                            '  "V_MESSAGE" ' +
                            'WHERE ' +
                            '  "V_MESSAGE"."FROM_USER_USERNAME" = $1 ',
                        values: [
                            from
                        ]
                    }, function(err, result) {
                        done();
                        var messages = result ? result.rows : null;
                        callback(err, messages);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a latest message for the user
         *
         * @name findLatestMessageByReceiver
         * @memberof datastore/postgres
         * @function
         * @public
         * @param to {String} The username
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findLatestMessageByReceiver: function(to, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findLatestMessageByReceiver',
                        text:
                            'SELECT ' +
                            '  "V_MESSAGE".* ' +
                            'FROM ' +
                            '  "V_MESSAGE" ' +
                            'WHERE ' +
                            '  "V_MESSAGE"."TO_USER_USERNAME" = $1 AND ' +
                            '  "V_MESSAGE"."STARTED" <= NOW() AND ' +
                            '  ("V_MESSAGE"."EXPIRED" IS NULL OR "V_MESSAGE"."EXPIRED" > NOW()) AND ' +
                            '  "V_MESSAGE"."MESSAGE_STATUS_NAME" IN ($2, $3) ' +
                            '  ORDER BY ' +
                            '  "V_MESSAGE"."STARTED" DESC, ' +
                            '  "V_MESSAGE"."EXPIRED" DESC ' +
                            'LIMIT ' +
                            '  1 ',
                        values: [
                            to,
                            VALID_MESSAGE_STATUSES[0], // unread
                            VALID_MESSAGE_STATUSES[1]  // read

                        ]
                    }, function(err, result) {
                        done();
                        var message = result ? result.rows[0] : null;
                        callback(err, message);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },
        /**
         * Search for a latest tip message for the user
         *
         * @name findLatestTipMessageByReceiver
         * @memberof datastore/postgres
         * @function
         * @public
         * @param to {String} The username
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findLatestTipMessageByReceiver: function(to, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findLatestTipMessageByReceiver',
                        text:
                            'SELECT ' +
                            '  "V_MESSAGE".* ' +
                            'FROM ' +
                            '  "V_MESSAGE" ' +
                            'WHERE ' +
                            '  "V_MESSAGE"."TO_USER_USERNAME" = $1 AND ' +
                            '  "V_MESSAGE"."STARTED" <= NOW() AND ' +
                            '  ("V_MESSAGE"."EXPIRED" IS NULL OR "V_MESSAGE"."EXPIRED" > NOW()) AND ' +
                            '  "V_MESSAGE"."MESSAGE_STATUS_NAME" IN ($2, $3) AND ' +
                            '  "V_MESSAGE"."MESSAGE_TYPE_NAME" = $4 ' +
                            'ORDER BY ' +
                            '  "V_MESSAGE"."STARTED" DESC, ' +
                            '  "V_MESSAGE"."EXPIRED" DESC ' +
                            'LIMIT ' +
                            '  1 ',
                        values: [
                            to,
                            VALID_MESSAGE_STATUSES[0],
                            VALID_MESSAGE_STATUSES[1],
                            'tip'
                        ]
                    }, function(err, result) {
                        done();
                        var message = result ? result.rows[0] : null;
                        callback(err, message);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update billing details
         *
         * @name addOrUpdateBillingDetails
         * @memberof datastore
         * @function
         * @public
         * @param billing_details {Object} Billing Details to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateBillingDetails: function(billing_details, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (billing_details.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateBillingDetails',
                            text:
                                'UPDATE ' +
                                '  "BILLING_DETAILS" ' +
                                'SET ' +
                                '  "ADDRESS_ID" = $1, ' +
                                '  "ELECTRICITY_DISTRIBUTOR" = $2, ' +
                                '  "ELECTRICITY_RETAILER" = $3, ' +
                                '  "BILLING_PERIOD" = $4, ' +
                                '  "LAST_BILLING_DATE" = $5, ' +
                                '  "USER_BILLING_PERIOD" = $6 ' +
                                'WHERE ' +
                                '  "ID" = $7 ',
                            values: [
                                billing_details.address ? billing_details.address.id : null,
                                billing_details.electricity_distributor,
                                billing_details.electricity_retailer,
                                billing_details.billing_period,
                                billing_details.last_billing_date,
                                billing_details.user_billing_period,
                                billing_details.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addBillingDetails',
                            text:
                                'INSERT INTO "BILLING_DETAILS"( ' +
                                '  "ADDRESS_ID", ' +
                                '  "ELECTRICITY_DISTRIBUTOR", ' +
                                '  "ELECTRICITY_RETAILER", ' +
                                '  "BILLING_PERIOD", ' +
                                '  "LAST_BILLING_DATE", ' +
                                '  "USER_BILLING_PERIOD" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                billing_details.address ? billing_details.address.id : null,
                                billing_details.electricity_distributor,
                                billing_details.electricity_retailer,
                                billing_details.billing_period,
                                billing_details.last_billing_date,
                                billing_details.user_billing_period
                            ]
                        }, function(err, result) {
                            if (!err) {
                                billing_details.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update an smart meter
         *
         * @name addOrUpdateSmartMeter
         * @memberof datastore
         * @function
         * @public
         * @param smart_meter {object} The smart meter to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateSmartMeter: function(smart_meter, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (smart_meter.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateSmartMeter',
                            text:
                                'UPDATE ' +
                                '  "SMART_METER" ' +
                                'SET ' +
                                '  "DEVICE_ID" = $1, ' +
                                '  "BILLING_DETAILS_ID" = $2, ' +
                                '  "PAIRING_WINDOW_START" = $3, ' +
                                '  "PAIRING_WINDOW_LENGTH" = $4, ' +
                                '  "PAIRED" = $5, ' +
                                '  "ONLINE" = $6, ' +
                                '  "FIRST_PAIRED" = $7 ' +
                                'WHERE ' +
                                '  "ID" = $8 ',
                            values: [
                                smart_meter.device.id,
                                smart_meter.billing_details ? smart_meter.billing_details.id : null,
                                smart_meter.pairing_window_start,
                                smart_meter.pairing_window_length,
                                smart_meter.paired,
                                smart_meter.online,
                                smart_meter.first_paired,
                                smart_meter.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addSmartMeter',
                            text:
                                'INSERT INTO "SMART_METER"( ' +
                                '  "DEVICE_ID", ' +
                                '  "BILLING_DETAILS_ID", ' +
                                '  "PAIRING_WINDOW_START", ' +
                                '  "PAIRING_WINDOW_LENGTH", ' +
                                '  "PAIRED", ' +
                                '  "ONLINE", ' +
                                '  "FIRST_PAIRED" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                smart_meter.device.id,
                                smart_meter.billing_details ? smart_meter.billing_details.id : null,
                                smart_meter.pairing_window_start,
                                smart_meter.pairing_window_length,
                                smart_meter.paired,
                                smart_meter.online,
                                smart_meter.first_paired
                            ]
                        }, function(err, result) {
                            if (!err) {
                                smart_meter.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the smart meter for the given HPC.
         *
         * @name findSmartMeterByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findSmartMeterByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSmartMeterByHpcID',
                        text:
                            'SELECT ' +
                            '  "V_SMART_METER".* ' +
                            'FROM ' +
                            '  "V_SMART_METER" ' +
                            '   LEFT JOIN "HPC" ON "V_SMART_METER"."ID" = "HPC"."SMART_METER_ID" ' +
                            'WHERE ' +
                            '  "HPC"."ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var smart_meter = result && result.rows ? result.rows[0] : null;
                        callback(err, smart_meter);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Clean SmartMeter on Unlink/Reset/Unpair
         *
         * @name cleanSmartMeter
         * @memberof datastore
         * @function
         * @public
         * @param sm_id {Number} The identifier of the SmartMeter to clean
         * @param callback Callback to fire when finished
         */
        cleanSmartMeter: function(sm_id, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    var status = 'Unknown';
                    clientQuery(client, true, {
                        name: 'cleanSmartMeter',
                        text:
                            'UPDATE ' +
                            '  "SMART_METER" ' +
                            'SET ' +
                            '  "CREATED" = NOW(), ' +
                            '  "PAIRED" = $1, ' +
                            '  "ONLINE" = NULL, ' +
                            '  "FIRST_PAIRED" = NULL ' +
                            'WHERE ' +
                            '  "ID" = $2 ',
                        values: [
                            status,
                            sm_id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Clean SmartMeter Device on Unlink/Reset/Unpair
         *
         * @name cleanSmartMeterDevice
         * @memberof datastore
         * @function
         * @public
         * @param sm_id {Number} The identifier of the SmartMeter to clean
         * @param callback {Function} Callback with err param
         */
        cleanSmartMeterDevice: function(sm_id, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'cleanSmartMeterDevice',
                        text:
                            'UPDATE ' +
                            '  "DEVICE" ' +
                            'SET ' +
                            '  "DEVICE_TYPE_ID" = dt."ID" ' +
                            'FROM "DEVICE" d ' +
                            'JOIN "SMART_METER" sm ' +
                            'ON d."ID"=sm."DEVICE_ID" ' +
                            'CROSS JOIN "DEVICE_TYPE" dt ' +
                            'WHERE ' +
                            '  d."ID" = "DEVICE"."ID" ' +
                            'AND ' +
                            '  sm."ID" = $1 ' +
                            'AND ' +
                            '  dt."NAME" = $2 ' +
                            'AND ' +
                            '  dt."MANUFACTURER" = $3',
                        values: [
                            sm_id,
                            // Defaults
                            'Unknown',
                            'Unknown'
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Clean SmartMeter Device Transport on Unlink/Reset/Unpair
         *
         * @name cleanSmartMeterDeviceTransport
         * @memberof datastore
         * @function
         * @public
         * @param sm_id {Number} The identifier of the SmartMeter to clean
         * @param callback {Function} Callback with err param
         */
        cleanSmartMeterDeviceTransport: function(sm_id, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    var default_transport_type_name = 'ZigBee SE';
                    var default_data = {
                        "nwk": "FFFF",
                        "node": "FFFFFFFFFFFFFFFF",
                        "endpoint": "FF"
                    };
                    clientQuery(client, true, {
                        name: 'cleanSmartMeterDeviceTransport',
                        text:
                            'UPDATE ' +
                            '  "TRANSPORT" ' +
                            'SET ' +
                            '  "TRANSPORT_TYPE_ID" = tt."ID", ' +
                            '  "DATA" = $3 ' +
                            'FROM "TRANSPORT" t ' +
                            'JOIN "DEVICE" d ' +
                            'ON t."ID"=d."TRANSPORT_ID" ' +
                            'JOIN "SMART_METER" sm ' +
                            'ON d."ID"=sm."DEVICE_ID" ' +
                            'CROSS JOIN "TRANSPORT_TYPE" tt ' +
                            'WHERE ' +
                            '  t."ID" = "TRANSPORT"."ID" ' +
                            'AND ' +
                            '  t."ID" = d."TRANSPORT_ID" ' +
                            'AND ' +
                            '  d."ID" = sm."DEVICE_ID" ' +
                            'AND ' +
                            '  sm."ID" = $1 ' +
                            'AND ' +
                            '  tt."NAME" = $2',
                        values: [
                            sm_id,
                            default_transport_type_name,
                            default_data
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a smart meter with the ID
         *
         * @name findSmartMeterByID
         * @memberof datastore
         * @function
         * @public
         * @param id {String} The ID of the desired smart meter
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findSmartMeterByID: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSmartMeterByID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_SMART_METER" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();

                        var smart_meter = result && result.rows ? result.rows[0] : null;
                        callback(err, smart_meter);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * List all device classes
         *
         * @name findAllDeviceClasses
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAllDeviceClasses: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAllDeviceClasses',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "DEVICE_CLASS" ',
                        values: []
                    }, function(err, result) {
                        done();
                        var details = result && result.rows || [];
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a device class with the tag
         *
         * @name findDeviceClassByTag
         * @memberof datastore
         * @function
         * @public
         * @param tag {String} The tag of the device class
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findDeviceClassByTag: function(tag, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findDeviceClassByTag',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "DEVICE_CLASS" ' +
                            'WHERE ' +
                            '  "TAG" = $1 ',
                        values: [
                            tag
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a device type with the supplied name and manufacturer
         *
         * @name findByNameAndManufacturer
         * @memberof datastore
         * @function
         * @public
         * @param name {String} The name of the device
         * @param manufacturer {String} The manufacturer of the device
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findDeviceTypeByNameAndManufacturer: function(name, manufacturer, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findDeviceTypeByNameAndManufacturer',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_DEVICE_TYPE" ' +
                            'WHERE ' +
                            '  "DEVICE_TYPE_NAME" = $1 AND ' +
                            '  "DEVICE_TYPE_MANUFACTURER" = $2 ',
                        values: [
                            name,
                            manufacturer
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a device type with the supplied manufacturer and model
         *
         * @name findDeviceTypeByManufacturerAndModel
         * @memberof datastore
         * @function
         * @public
         * @param manufacturer {String} The manufacturer of the device
         * @param model {Object} The model of the device
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findDeviceTypeByManufacturerAndModel: function(manufacturer, model, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findDeviceTypeByManufacturerAndModel',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_DEVICE_TYPE" ' +
                            'WHERE ' +
                            '  lower(trim(from "DEVICE_TYPE_MANUFACTURER")) = lower(trim(from $1)) AND ' +
                            '  lower(trim(from "DEVICE_TYPE_MODEL")) = lower(trim(from $2)) ',
                        values: [
                            manufacturer,
                            model
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a device type with the supplied manufacturer and description
         *
         * @name findDeviceTypeByManufacturerAndDescription
         * @memberof datastore
         * @function
         * @public
         * @param manufacturer {String} The manufacturer of the device
         * @param description {Object} The description of the device
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findDeviceTypeByManufacturerAndDescription: function(manufacturer, description, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findDeviceTypeByManufacturerAndDescription',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_DEVICE_TYPE" ' +
                            'WHERE ' +
                            '  lower("DEVICE_TYPE_MANUFACTURER") = lower($1) AND ' +
                            '  lower("DEVICE_TYPE_DESCRIPTION"::varchar) = lower($2) ',
                        values: [
                            manufacturer,
                            JSON.stringify(description)
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a transport type with the supplied name
         *
         * @name findTransportTypeByName
         * @memberof datastore
         * @function
         * @public
         * @param name {String} The name of the transport
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findTransportTypeByName: function(name, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findTransportTypeByName',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "TRANSPORT_TYPE" ' +
                            'WHERE ' +
                            '  "NAME" = $1 ',
                        values: [
                            name
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a icon type with the CSS class
         *
         * @name findIconIdForCssClass
         * @memberof datastore
         * @function
         * @public
         * @param css_class {String} Css class for the HA Icon
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findIconIdForCssClass: function(css_class, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findIconIdForCssClass',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "HOME_AUTOMATION_ICON" ' +
                            'WHERE ' +
                            '  "CSS_CLASS" = $1 ',
                        values: [
                            css_class
                        ]
                    }, function(err, result) {
                        done();
                        var details = result && result.rows ? result.rows[0] : null;
                        callback(err, details);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a device
         *
         * @name addOrUpdateDevice
         * @memberof datastore
         * @function
         * @public
         * @param device {object} The device to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateDevice: function(device, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    if (device.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateDevice',
                            text:
                                'UPDATE ' +
                                '  "DEVICE" ' +
                                'SET ' +
                                '  "DEVICE_TYPE_ID" = $1, ' +
                                '  "DEVICE_INFO_ID" = $2, ' +
                                '  "TRANSPORT_ID" = $3 ' +
                                'WHERE ' +
                                '  "ID" = $4 ',
                            values: [
                                device.type.id,
                                device.info ? device.info.id : null,
                                device.transport.id,
                                device.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addDevice',
                            text:
                                'INSERT INTO "DEVICE"( ' +
                                '  "DEVICE_TYPE_ID", ' +
                                '  "DEVICE_INFO_ID", ' +
                                '  "TRANSPORT_ID" ' +
                                ') VALUES (' +
                                '  $1, $2, $3 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                device.type.id,
                                device.info ? device.info.id : null,
                                device.transport.id
                            ]
                        }, function(err, result) {
                            if (!err) {
                                device.id = result.rows[0].ID;
                            }
                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a device
         *
         * @name deleteDevice
         * @memberof datastore.postgres
         * @function
         * @public
         * @param device {object} The device to delete
         * @param callback {Function} Callback to fire when finished
         */
        deleteDevice: function(device, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteDevice',
                        text:
                            'DELETE FROM ' +
                            '  "DEVICE" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            device.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a device info
         *
         * @name addOrUpdateDeviceInfo
         * @memberof datastore.postgres
         * @function
         * @public
         * @param info {Object} A device info to save
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateDeviceInfo: function(info, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    if (info.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateDeviceInfo',
                            text:
                                'UPDATE ' +
                                '  "DEVICE_INFO" ' +
                                'SET ' +
                                '  "DATA" = $1 ' +
                                'WHERE ' +
                                '  "ID" = $2 ',
                            values: [
                                info.data,
                                info.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addDeviceInfo',
                            text:
                                'INSERT INTO "DEVICE_INFO"( ' +
                                '  "DATA" ' +
                                ') VALUES ( ' +
                                '  $1 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                info.data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                info.id = result.rows[0].ID;
                            }
                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a device info
         *
         * @name deleteDeviceInfo
         * @memberof datastore.postgres
         * @function
         * @public
         * @param info {Object} A device info to delete
         * @param callback {Function} A callback to fire when finished
         */
        deleteDeviceInfo: function(info, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteDeviceInfo',
                        text:
                            'DELETE FROM ' +
                            '  "DEVICE_INFO" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            info.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a transport
         *
         * @name addOrUpdateTransport
         * @memberof datastore
         * @function
         * @public
         * @param transport {object} The transport to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateTransport: function(transport, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    if (transport.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateTransport',
                            text:
                                'UPDATE ' +
                                '  "TRANSPORT" ' +
                                'SET ' +
                                '  "TRANSPORT_TYPE_ID" = $1, ' +
                                '  "DATA" = $2 ' +
                                'WHERE ' +
                                '  "ID" = $3 ',
                            values: [
                                transport.type.id,
                                transport.data,
                                transport.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addTransport',
                            text:
                                'INSERT INTO "TRANSPORT"( ' +
                                '  "TRANSPORT_TYPE_ID", ' +
                                '  "DATA" ' +
                                ') VALUES ( ' +
                                '  $1, $2 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                transport.type.id,
                                transport.data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                transport.id = result.rows[0].ID;
                            }
                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a transport
         *
         * @name deleteTransport
         * @memberof datastore.postgres
         * @function
         * @public
         * @param transport {object} The transport to delete
         * @param callback {Function} Callback to fire when finished
         */
        deleteTransport: function(transport, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteTransport',
                        text:
                            'DELETE FROM ' +
                            '  "TRANSPORT" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            transport.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a location nearest to the supplied latitude/longitude
         *
         * @name findNearestLocation
         * @memberof datastore
         * @function
         * @public
         * @param lat {Number} The latitude to search near
         * @param long {Number} The longitude to search near
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findNearestLocation: function(lat, long, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findNearestLocation',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_LOCATION" ' +
                            'WHERE ' +
                            '  "LATLONG" IS NOT NULL ' +
                            'ORDER BY ' +
                            '  "LATLONG" <-> ST_SetSRID(ST_MakePoint($1, $2), 4236) ' +
                            'LIMIT ' +
                            '  1 ',
                        values: [
                            lat,
                            long
                        ]
                    }, function(err, result) {
                        done();

                        var hpc = result && result.rows ? result.rows[0] : null;
                        callback(err, hpc);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for locations without latitude/longitude
         *
         * @name findLocationsWithoutLatLong
         * @memberof datastore
         * @function
         * @public
         * @param limit {Number} The maximum number of results to retrieve
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findLocationsWithoutLatLong: function(limit, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findLocationsWithoutLatLong',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_LOCATION" ' +
                            'WHERE ' +
                            '  "LATLONG" IS NULL ' +
                            'LIMIT ' +
                            '  $1 ',
                        values: [
                            limit
                        ]
                    }, function(err, result) {
                        done();

                        var locs = result && result.rows ? result.rows : [];
                        callback(err, locs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a tariff
         *
         * @name addOrUpdateTariff
         * @memberof datastore
         * @function
         * @public
         * @param tariff {object} The tariff to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateTariff: function(tariff, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (tariff.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateTariff',
                            text:
                                'UPDATE ' +
                                '  "TARIFF" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "CREATED_BY_USER_ID" = $2, ' +
                                '  "CREATED_BY_SMART_METER_ID" = $3, ' +
                                '  "VALID_FROM" = $4, ' +
                                '  "VALID_UNTIL" = $5, ' +
                                '  "TARIFF_DATA" = $6, ' +
                                '  "COMPLEX_TARIFF_DATA" = $7 ' +
                                'WHERE ' +
                                '  "ID" = $8 ',
                            values: [
                                tariff.hpc_id,
                                tariff.created_by_user_id,
                                tariff.created_by_smart_meter_id,
                                tariff.valid_from,
                                tariff.valid_until,
                                tariff.tariff_data,
                                tariff.complex_tariff_data,
                                tariff.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addTariff',
                            text:
                                'INSERT INTO "TARIFF"( ' +
                                '  "HPC_ID", ' +
                                '  "CREATED_BY_USER_ID", ' +
                                '  "CREATED_BY_SMART_METER_ID", ' +
                                '  "VALID_FROM", ' +
                                '  "VALID_UNTIL", ' +
                                '  "TARIFF_DATA", ' +
                                '  "COMPLEX_TARIFF_DATA" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                tariff.hpc_id,
                                tariff.created_by_user_id,
                                tariff.created_by_smart_meter_id,
                                tariff.valid_from,
                                tariff.valid_until,
                                tariff.tariff_data,
                                tariff.complex_tariff_data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                tariff.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all tariffs created by the user
         *
         * @name deleteTariffsByUsername
         * @memberof datastore.postgres
         * @function
         * @public
         * @param username {String} The username
         * @param callback Callback to fire when finished
         */
        deleteTariffsByUsername: function(username, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteTariffsByUsername',
                        text:
                            'DELETE FROM ' +
                            '  "TARIFF" ' +
                            'WHERE ' +
                            '  "CREATED_BY_USER_ID" IN (' +
                            '    SELECT ' +
                            '      "ID" ' +
                            '    FROM ' +
                            '      "USER" ' +
                            '    WHERE ' +
                            '      "USERNAME" = $1 ' +
                            '  )',
                        values: [
                            username
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the current tariff for the supplied HPC and/or the
         * supplied user
         *
         * @name findTariffForHPCForUserAtTime
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param user_id {Number} The ID of the user who created user tariff
         * @param timestamp {Date} The date/time at which the tariff should apply
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findTariffForHPCForUserAtTime: function(hpc_id, user_id, timestamp, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findTariffForHPCForUserAtTime' + (hpc_id ? '_withHpcId' : '_nullHpcId'),
                        text:
                            hpc_id ?
                                ('SELECT ' +
                                 '  * ' +
                                 'FROM ' +
                                 '  "TARIFF" ' +
                                 'WHERE ' +
                                 '  "HPC_ID" = $1 AND ' +
                                 '  "VALID_FROM" <= $2 AND ' +
                                 '  ("VALID_UNTIL" IS NULL OR "VALID_UNTIL" > $3) AND ' +
                                 '  ("CREATED_BY_USER_ID" IS NULL OR "CREATED_BY_USER_ID" = $4) ' +
                                 'ORDER BY ' +
                                 '  "CREATED_BY_USER_ID" ') :
                                ('SELECT ' +
                                 '  * ' +
                                 'FROM ' +
                                 '  "TARIFF" ' +
                                 'WHERE ' +
                                 '  "HPC_ID" IS NULL AND ' +
                                 '  "VALID_FROM" <= $1 AND ' +
                                 '  ("VALID_UNTIL" IS NULL OR "VALID_UNTIL" > $2) AND ' +
                                 '  "CREATED_BY_USER_ID" = $3 ' +
                                 'ORDER BY ' +
                                 '  "CREATED_BY_USER_ID" '),
                        values: hpc_id ? [
                            hpc_id,
                            timestamp,
                            timestamp,
                            user_id
                        ] : [
                            timestamp,
                            timestamp,
                            user_id
                        ]
                    }, function(err, result) {
                        done();
                        var tariff = result && result.rows ? result.rows[0] : null;
                        callback(err, tariff);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the current meter-created tariff for the supplied HPC
         *
         * @name findMeterTariffForHPCAtTime
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param timestamp {Date} The date/time at which the tariff should apply
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findMeterTariffForHPCAtTime: function(hpc_id, timestamp, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findMeterTariffForHPCAtTime',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "TARIFF" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "VALID_FROM" <= $2 AND ' +
                            '  ("VALID_UNTIL" > $3 OR "VALID_UNTIL" IS NULL) AND ' +
                            '  "CREATED_BY_SMART_METER_ID" IS NOT NULL ',
                        values: [
                            hpc_id,
                            timestamp,
                            timestamp
                        ]
                    }, function(err, result) {
                        done();
                        var tariff = result && result.rows ? result.rows[0] : null;
                        callback(err, tariff);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all the tariff for the supplied HPC
         *
         * @name findTariffsByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findTariffsByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findTariffsByHpcID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "TARIFF" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var tariffs = result && result.rows ? result.rows : [];
                        callback(err, tariffs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Link the tariff with an user id user with the supplied HPC
         *
         * @name linkHpcWithTariffForUser
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param user_id {Number} The ID of the user who created user tariff
         * @param callback {Function} A callback with the usual error and result parameters
         */
        linkHpcWithTariffForUser: function(hpc_id, user_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // update on user_id
                    clientQuery(client, true, {
                        name: 'linkHpcWithTariffForUser',
                        text:
                            'UPDATE ' +
                            '  "TARIFF" ' +
                            'SET ' +
                            '  "HPC_ID" = $1 ' +
                            'WHERE ' +
                            ' "CREATED_BY_USER_ID" = $2 AND ' +
                            ' ("HPC_ID" IS NULL) ',
                        values: [
                            hpc_id,
                            user_id,
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Stop tariffs that's valid in the future, and was created by
         * a specified user or by the smart meter.
         *
         * @name stopFutureTariff
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The ID of the hpc
         * @param user_id {Number} The ID of the user who created the tariff
         *                         (optional)
         * @param callback Callback to fire when finished
         */
        stopFutureTariff: function(hpc_id, user_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var with_user = user_id !== undefined;
                    var query =
                        'UPDATE ' +
                        '  "TARIFF" ' +
                        'SET ' +
                        '  "VALID_UNTIL" = NOW() ' +
                        'WHERE ' +
                        '  "HPC_ID" = $1 AND ' +
                        '  ("VALID_UNTIL" IS NULL OR "VALID_UNTIL" > NOW()) AND ';
                    var params = [hpc_id];
                    // todo: Why do we care about user ID here?
                    if (with_user) {
                        query += '  ("CREATED_BY_USER_ID" IS NULL OR "CREATED_BY_USER_ID" = $2) ';
                        params.push(user_id);
                    } else {
                        query += '  ("CREATED_BY_USER_ID" IS NULL) ';
                    }
                    clientQuery(client, true, {
                        name: 'stopFutureTariff' + (with_user ? 'User' : ''),
                        text: query,
                        values: params
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }

            });
        },

        /**
         * Insert/update a retailer tariff
         *
         * @name addOrUpdateRetailerTariff
         * @memberof datastore
         * @function
         * @public
         * @param tariff {object} The retailer tariff to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateRetailerTariff: function(tariff, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (tariff.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateRetailerTariff',
                            text:
                                'UPDATE ' +
                                '  "RETAILER_TARIFF" ' +
                                'SET ' +
                                '  "DISTRIBUTOR" = $1, ' +
                                '  "RETAILER" = $2, ' +
                                '  "COMPLEX_TARIFF_DATA" = $3, ' +
                                '  "NAME" = $4, ' +
                                '  "TEST_ONLY" = $5, ' +
                                '  "EXPIRED" = $6, ' +
                                '  "NAMESPACE" = $7 ' +
                                'WHERE ' +
                                '  "ID" = $8 ',
                            values: [
                                tariff.distributor,
                                tariff.retailer,
                                tariff.complex_tariff_data,
                                tariff.name,
                                tariff.test_only,
                                tariff.expired,
                                tariff.namespace,
                                tariff.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addRetailerTariff',
                            text:
                                'INSERT INTO "RETAILER_TARIFF"( ' +
                                '  "DISTRIBUTOR", ' +
                                '  "RETAILER", ' +
                                '  "COMPLEX_TARIFF_DATA", ' +
                                '  "NAME", ' +
                                '  "TEST_ONLY", ' +
                                '  "EXPIRED", ' +
                                '  "NAMESPACE" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                tariff.distributor,
                                tariff.retailer,
                                tariff.complex_tariff_data,
                                tariff.name,
                                tariff.test_only,
                                tariff.expired,
                                tariff.namespace
                            ]
                        }, function(err, result) {
                            if (!err) {
                                tariff.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a retailer tariff identified by the ID scrapped from VIC
         * tariff plan
         *
         * @name findRetailerTariffByOfferID
         * @memberof datastore
         * @function
         * @public
         * @param id {Number} The private identifier of the retailer tariff used
         * on the switchon.vic.gov.au website, plus a percent string of green
         * power.
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRetailerTariffByOfferID: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findRetailerTariffByOfferID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RETAILER_TARIFF" ' +
                            'WHERE ' +
                            '  "COMPLEX_TARIFF_DATA"::json->>\'id\' = $1 ' +
                            (connection.includes_retailer_tariff_test_data ? '' : ' AND "TEST_ONLY" IS NOT true'),
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();
                        var tariff = result && result.rows ? result.rows[0] : null;
                        callback(err, tariff);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a retailer tariff identified by the supplied value
         *
         * @name findRetailerTariffByID
         * @memberof datastore
         * @function
         * @public
         * @param id {Number} The private identifier of the retailer tariff
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRetailerTariffByID: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findRetailerTariffByID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RETAILER_TARIFF" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();
                        var tariff = result && result.rows ? result.rows[0] : null;
                        callback(err, tariff);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the retailer tariffs with the supplied distributor and retailer
         *
         * @name findRetailerTariffsByUtility
         * @memberof datastore
         * @function
         * @public
         * @param distributor {String} The name of the distributor, used to narrow the search
         * @param retailer {String} The name of the retailer, used to narrow the search
         * @param options {Object} Options for paging and filtering, with:
         *                         offset {Number} Used in paging
         *                         limit {Number} Used in paging
         *                         expired {Boolean} Want expired tariff in results
         *                         solar-only {Boolean} Want only solar compatible tariffs in the results
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRetailerTariffsByUtility: function(distributor, retailer, options, callback) {
            /*jshint -W071 */
            /*jshint -W074 */
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var statement = {
                        name: 'findRetailerTariffsByUtility',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RETAILER_TARIFF" ',
                        values: []
                    };
                    var where = '';

                    if (distributor && distributor !== 'Any') {
                        statement.name += '_whereD';
                        where +=
                            'WHERE ' +
                            '  "DISTRIBUTOR" = $1 ';
                        statement.values.push(distributor);
                    }
                    if (retailer && retailer !== 'Any') {
                        if (where) {
                            statement.name += '_andR';
                            where +=
                                '  AND "RETAILER" = $2 ';
                        } else {
                            statement.name += '_whereR';
                            where +=
                                'WHERE ' +
                                '  "RETAILER" = $1 ';
                        }
                        statement.values.push(retailer);
                    }
                    if (!connection.includes_retailer_tariff_test_data) {
                        where += (where ? 'AND ' : 'WHERE ') +
                            '"TEST_ONLY" IS NOT true ';
                    }
                    if (options['solar-only']) {
                        where += (where ? 'AND ' : 'WHERE ') +
                            '("COMPLEX_TARIFF_DATA"->>\'for_solar\')::boolean = $' + (statement.values.length + 1) + ' ';
                        statement.name += '_solarOnly';
                        statement.values.push(options['solar-only']);
                    }
                    if (!options.expired) {
                        if (where) {
                            statement.name += '_andE';
                            where +=
                                '  AND ("COMPLEX_TARIFF_DATA"::json->>\'expired\') IS NULL ';
                        } else {
                            statement.name += '_whereE';
                            where +=
                                'WHERE ' +
                                '  ("COMPLEX_TARIFF_DATA"::json->>\'expired\') IS NULL ';
                        }
                    }

                    statement.text += where +
                        'ORDER BY ' +
                        '  "NAME" ';

                    if (options.limit) {
                        statement.name += '_withLimit';
                        statement.text +=
                            'OFFSET ' +
                            '  $' + (statement.values.length + 1) + ' ' +
                            'LIMIT ' +
                            '  $' + (statement.values.length + 2) + ' ';
                        statement.values.push(options.offset, options.limit);
                    }

                    clientQuery(client, false, statement, function(err, result) {
                        done();

                        var srs = result && result.rows ? result.rows : [];
                        callback(err, srs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
            /*jshint +W074 */
            /*jshint +W071 */
        },

        /**
         * Count the retailer tariffs with the supplied distributor and retailer
         *
         * @name countRetailerTariffsByUtility
         * @memberof datastore
         * @function
         * @public
         * @param distributor {String} The name of the distributor, used to narrow the search
         * @param retailer {String} The name of the retailer, used to narrow the search
         * @param options {Object} Options used to filter counted result, currently supports:
         *                         expired {Boolean} Want expired tariffs counted
         * @param callback {Function} A callback with the usual error and result parameters
         */
        countRetailerTariffsByUtility: function(distributor, retailer, options, callback) {
            /*jshint -W071 */
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var statement = {
                        name: 'countRetailerTariffsByUtility',
                        text:
                            'SELECT ' +
                            '  COUNT(*) AS COUNT ' +
                            'FROM ' +
                            '  "RETAILER_TARIFF" ',
                        values: []
                    };
                    var where = '';

                    // todo: Some of this could be factored out with
                    //       findRetailerTariffsByUtility.
                    if (distributor && distributor !== 'Any') {
                        statement.name += '_whereD';
                        where +=
                            'WHERE ' +
                            '  "DISTRIBUTOR" = $1 ';
                        statement.values.push(distributor);
                    }
                    if (retailer && retailer !== 'Any') {
                        if (where) {
                            statement.name += '_andR';
                            where +=
                                '  AND "RETAILER" = $2 ';
                        } else {
                            statement.name += '_whereR';
                            where +=
                                'WHERE ' +
                                '  "RETAILER" = $1 ';
                        }
                        statement.values.push(retailer);
                    }
                    if (!options.expired) {
                        if (where) {
                            statement.name += '_andE';
                            where += 'AND ';
                        } else {
                            statement.name += '_whereE';
                            where += 'WHERE ';
                        }
                        where += '"EXPIRED" IS NOT NULL ';
                    }
                    if (!connection.includes_retailer_tariff_test_data) {
                        where += (where ? 'AND ' : 'WHERE ') +
                            '"TEST_ONLY" IS NOT true ';
                    }

                    statement.text += where;

                    clientQuery(client, false, statement, function(err, result) {
                        done();

                        var count = result && result.rows && result.rows.length ? result.rows[0].count : 0;
                        callback(err, count);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the retailer tariffs with the supplied distributor and retailer and name
         *
         * @name findRetailerTariffsByNameAndUtility
         * @memberof datastore
         * @function
         * @public
         * @param distributor {String} The name of the distributor, used to narrow the search
         * @param retailer {String} The name of the retailer, used to narrow the search
         * @param name {String} The name of the tariff
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRetailerTariffsByNameAndUtility: function(distributor, retailer, name, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  * ' +
                        'FROM ' +
                        '  "RETAILER_TARIFF" ' +
                        'WHERE ' +
                        '  "DISTRIBUTOR" = $1 AND ' +
                        '  "RETAILER" = $2 AND ' +
                        '  "NAME" = $3 ' +
                        (connection.includes_retailer_tariff_test_data ?
                         '' : ' AND "TEST_ONLY" IS NOT true');

                    clientQuery(client, false, {
                        name: 'findRetailerTariffsByNameAndUtility',
                        text: sql,
                        values: [
                            distributor,
                            retailer,
                            name
                        ]
                    }, function(err, result) {
                        done();

                        var tariff = result && result.rows ? result.rows[0] : null;
                        callback(err, tariff);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the address for the supplied HPC
         *
         * @name findAddressByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with error and result params
         *                            The result param is an address DTO,
         *                            or undefined if there is no smart meter
         *                            entry for the given HPC.
         */
        findAddressByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAddressByHpcID',
                        text:
                            'SELECT ' +
                            '  "V_ADDRESS".* ' +
                            'FROM ' +
                            '  "HPC" ' +
                            '  LEFT JOIN "SMART_METER" ON "SMART_METER"."ID" = "HPC"."SMART_METER_ID" ' +
                            '  LEFT JOIN "BILLING_DETAILS" ON "BILLING_DETAILS"."ID" = "SMART_METER"."BILLING_DETAILS_ID" ' +
                            '  LEFT JOIN "V_ADDRESS" ON "V_ADDRESS"."ID" = "BILLING_DETAILS"."ADDRESS_ID" ' +
                            'WHERE ' +
                            '  "HPC"."ID" = $1 ' +
                            // Note that there may be no SMART_METER entry
                            // for the window until a network/connection/state
                            // message is received from the HPC.
                            '  AND "V_ADDRESS"."ID" IS NOT NULL',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var address = result && result.rows ? result.rows[0] : null;
                        callback(err, address);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the address for the supplied location
         *
         * @name findAddressByLocationID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param location_id {Number} The private identifier of the location
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAddressByLocationID: function(location_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAddressByLocationID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_ADDRESS" ' +
                            'WHERE ' +
                            '  "NEAREST_LOCATION_ID" = $1 ',
                        values: [
                            location_id
                        ]
                    }, function(err, result) {
                        done();

                        var address = result && result.rows ? result.rows[0] : null;
                        callback(err, address);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update an address
         *
         * @name addOrUpdateAddress
         * @memberof datastore
         * @function
         * @public
         * @param address {object} The address to save
         *                         The id field will be set in-place for insert.
         * @param callback {Function} Callback with err param
         */
        addOrUpdateAddress: function(address, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (address.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateAddress',
                            text:
                                'UPDATE ' +
                                '  "ADDRESS" ' +
                                'SET ' +
                                '  "STREET" = $1, ' +
                                '  "CITY" = $2, ' +
                                '  "STATE" = $3, ' +
                                '  "COUNTRY" = $4, ' +
                                '  "POSTCODE" = $5, ' +
                                '  "NEAREST_LOCATION_ID" = $6 ' +
                                'WHERE ' +
                                '  "ID" = $7 ',
                            values: [
                                address.street,
                                address.city,
                                address.state,
                                address.country,
                                address.postcode,
                                address.location ? address.location.id : null,
                                address.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addAddress',
                            text:
                                'INSERT INTO "ADDRESS"( ' +
                                '  "STREET", ' +
                                '  "CITY", ' +
                                '  "STATE", ' +
                                '  "COUNTRY", ' +
                                '  "POSTCODE", ' +
                                '  "NEAREST_LOCATION_ID" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                address.street,
                                address.city,
                                address.state,
                                address.country,
                                address.postcode,
                                address.location ? address.location.id : null
                            ]
                        }, function(err, result) {
                            if (!err) {
                                address.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a budget attached to the supplied HPC and created by the
         * suppplied user
         *
         * @name findBudgetByHpcIDCreatedByUser
         * @memberof datastore.postgres
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param user_id {Number} The ID of the user who created the budget
         * @param callback Callback to fire when finished
         */
        findBudgetByHpcIDCreatedByUser: function(hpc_id, user_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findBudgetByHpcIDCreatedByUser',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "BUDGET" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "CREATED_BY_USER_ID" = $2 ',
                        values: [
                            hpc_id,
                            user_id
                        ]
                    }, function(err, result) {
                        done();

                        var budget = result && result.rows ? result.rows[0] : null;
                        callback(err, budget);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a budget attached to the supplied HPC
         *
         * @name findBudgetByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findBudgetByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findBudgetByHpcID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "BUDGET" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var budget = result && result.rows ? result.rows[0] : null;
                        callback(err, budget);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a budget
         *
         * @name addOrUpdateBudget
         * @memberof datastore
         * @function
         * @public
         * @param budget {object} The budget to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateBudget: function(budget, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (budget.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateBudget',
                            text:
                                'UPDATE ' +
                                '  "BUDGET" ' +
                                'SET ' +
                                '  "TOTAL" = $1, ' +
                                '  "ALERTS" = $2, ' +
                                '  "CREATED_BY_USER_ID" = $3, ' +
                                '  "CREATED" = $4, ' +
                                '  "HPC_ID" = $5 ' +
                                'WHERE ' +
                                '  "ID" = $6 ',
                            values: [
                                budget.total,
                                budget.alerts,
                                budget.created_by_user_id,
                                budget.created,
                                budget.hpc_id,
                                budget.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addBudget',
                            text:
                                'INSERT INTO "BUDGET"( ' +
                                '  "TOTAL", ' +
                                '  "ALERTS", ' +
                                '  "CREATED_BY_USER_ID", ' +
                                '  "CREATED", ' +
                                '  "HPC_ID" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                budget.total,
                                budget.alerts,
                                budget.created_by_user_id,
                                budget.created,
                                budget.hpc_id
                            ]
                        }, function(err, result) {
                            if (!err) {
                                budget.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all budgets created by the user
         *
         * @name deleteBudgetsByUsername
         * @memberof datastore.postgres
         * @function
         * @public
         * @param username {String} The username
         * @param callback Callback to fire when finished
         */
        deleteBudgetsByUsername: function(username, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteBudgetsByUsername',
                        text:
                            'DELETE FROM ' +
                            '  "BUDGET" ' +
                            'WHERE ' +
                            '  "CREATED_BY_USER_ID" IN (' +
                            '    SELECT ' +
                            '      "ID" ' +
                            '    FROM ' +
                            '      "USER" ' +
                            '    WHERE ' +
                            '      "USERNAME" = $1 ' +
                            '  )',
                        values: [
                            username
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a typical household model attached to the supplied HPC
         *
         * @name findTypicalHouseholdByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findTypicalHouseholdByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findTypicalHouseholdByHpcID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "TYPICAL_HOUSEHOLD" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var typical_household = result && result.rows ? result.rows[0] : null;
                        callback(err, typical_household);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a typical household model
         *
         * @name addOrUpdateTypicalHousehold
         * @memberof datastore
         * @function
         * @public
         * @param typical_household {object} The typical household model to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateTypicalHousehold: function(typical_household, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (typical_household.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateTypicalHousehold',
                            text:
                                'UPDATE ' +
                                '  "TYPICAL_HOUSEHOLD" ' +
                                'SET ' +
                                '  "CREATED_BY_USER_ID" = $1, ' +
                                '  "OCCUPANTS" = $2, ' +
                                '  "BEDROOMS" = $3, ' +
                                '  "POOL" = $4, ' +
                                '  "HOTWATER" = $5, ' +
                                '  "HEATING" = $6, ' +
                                '  "COOLING" = $7, ' +
                                '  "SPLITS" = $8, ' +
                                '  "COOKING" = $9, ' +
                                '  "REFRIGERATORS" = $10, ' +
                                '  "FREEZERS" = $11, ' +
                                '  "DISHWASHER" = $12, ' +
                                '  "WASHINGMACHINE" = $13, ' +
                                '  "CLOTHESDRYER" = $14, ' +
                                '  "LEDS" = $15, ' +
                                '  "NONLEDS" = $16 ' +
                                'WHERE ' +
                                '  "ID" = $17 ',
                            values: [
                                typical_household.created_by_user_id,
                                typical_household.occupants,
                                typical_household.bedrooms,
                                typical_household.pool,
                                typical_household.hotwater,
                                typical_household.heating,
                                typical_household.cooling,
                                typical_household.splits,
                                typical_household.cooking,
                                typical_household.refrigerators,
                                typical_household.freezers,
                                typical_household.dishwasher,
                                typical_household.washingmachine,
                                typical_household.clothesdryer,
                                typical_household.leds,
                                typical_household.nonleds,
                                typical_household.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addTypicalHousehold',
                            text:
                                'INSERT INTO "TYPICAL_HOUSEHOLD"( ' +
                                '  "CREATED_BY_USER_ID", ' +
                                '  "CREATED", ' +
                                '  "HPC_ID", ' +
                                '  "OCCUPANTS", ' +
                                '  "BEDROOMS", ' +
                                '  "POOL", ' +
                                '  "HOTWATER", ' +
                                '  "HEATING", ' +
                                '  "COOLING", ' +
                                '  "SPLITS", ' +
                                '  "COOKING", ' +
                                '  "REFRIGERATORS", ' +
                                '  "FREEZERS", ' +
                                '  "DISHWASHER", ' +
                                '  "WASHINGMACHINE", ' +
                                '  "CLOTHESDRYER", ' +
                                '  "LEDS", ' +
                                '  "NONLEDS" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                typical_household.created_by_user_id,
                                typical_household.created,
                                typical_household.hpc_id,
                                typical_household.occupants,
                                typical_household.bedrooms,
                                typical_household.pool,
                                typical_household.hotwater,
                                typical_household.heating,
                                typical_household.cooling,
                                typical_household.splits,
                                typical_household.cooking,
                                typical_household.refrigerators,
                                typical_household.freezers,
                                typical_household.dishwasher,
                                typical_household.washingmachine,
                                typical_household.clothesdryer,
                                typical_household.leds,
                                typical_household.nonleds
                            ]
                        }, function(err, result) {
                            if (!err) {
                                typical_household.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all typical household records created by the user
         *
         * @name deleteTypicalHouseholdsByUsername
         * @memberof datastore.postgres
         * @function
         * @public
         * @param username {String} The username
         * @param callback Callback to fire when finished
         */
        deleteTypicalHouseholdsByUsername: function(username, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteTypicalHouseholdsByUsername',
                        text:
                            'DELETE FROM ' +
                            '  "TYPICAL_HOUSEHOLD" ' +
                            'WHERE ' +
                            '  "CREATED_BY_USER_ID" IN (' +
                            '    SELECT ' +
                            '      "ID" ' +
                            '    FROM ' +
                            '      "USER" ' +
                            '    WHERE ' +
                            '      "USERNAME" = $1 ' +
                            '  )',
                        values: [
                            username
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all tip history records created for the user
         *
         * @name deleteExternalTipSourceHistoryByUsername
         * @memberof datastore
         * @function
         * @public
         * @param username {String} The username
         * @param callback Callback to fire when finished
         */
        deleteExternalTipSourceHistoryByUsername: function(username, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteExternalTipSourceHistoryByUsername',
                        text:
                            'DELETE FROM ' +
                            '  "APPLIANCE_TIP_HISTORY" ' +
                            'WHERE ' +
                            '  "USER_ID" IN (' +
                            '    SELECT ' +
                            '      "ID" ' +
                            '    FROM ' +
                            '      "USER" ' +
                            '    WHERE ' +
                            '      "USERNAME" = $1 ' +
                            '  )',
                        values: [
                            username
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for an app_install by its identifier
         *
         * @name findAppInstallById
         * @memberof datastore.postgres
         * @function
         * @public
         * @param id {Number} The identifier of the app
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAppInstallById: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAppInstallById',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_APP_INSTALL" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();
                        var app = result && result.rows ? result.rows[0] : null;
                        callback(err, app);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for an app_install attached to the supplied username
         *
         * @name findAppInstallByUsername
         * @memberof datastore.postgres
         * @function
         * @public
         * @param username {String} The username of the attached user
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAppInstallByUsername: function(username, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAppInstallByUsername',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_APP_INSTALL" ' +
                            'WHERE ' +
                            '  "USERNAME" = $1 ' +
                            'ORDER BY ' +
                            '  "CREATED" DESC ',
                        values: [
                            username
                        ]
                    }, function(err, result) {
                        done();
                        var app_installs = result && result.rows ? result.rows : [];
                        callback(err, app_installs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for an app_install by its external_install_id
         *
         * @name findAppInstallByInstallId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param iid {String} The external_install_id to search
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAppInstallByInstallId: function(iid, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAppInstallByInstallId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_APP_INSTALL" ' +
                            'WHERE ' +
                            '  "EXTERNAL_INSTALL_ID" = $1 ',
                        values: [
                            iid
                        ]
                    }, function(err, result) {
                        done();
                        var app = result && result.rows ? result.rows[0] : null;
                        callback(err, app);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update an app_install
         *
         * @name addOrUpdateAppInstall
         * @memberof datastore
         * @function
         * @public
         * @param app_install {object} The app_install to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateAppInstall: function(app_install, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (app_install.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateAppInstall',
                            text:
                                'UPDATE ' +
                                '  "APP_INSTALL" ' +
                                'SET ' +
                                '  "EXTERNAL_INSTALL_ID" = $1, ' +
                                '  "EXTERNAL_NOTIFICATION_SERVICE_ID" = $2, ' +
                                '  "CREATED" = $3, ' +
                                '  "NAME" = $4, ' +
                                '  "OS" = $5, ' +
                                '  "STATUS_ID" = $6, ' +
                                '  "USER_ID" = $7  ' +
                                'WHERE ' +
                                '  "ID" = $8 ',
                            values: [
                                app_install.external_install_id,
                                app_install.external_notification_service_id,
                                app_install.created,
                                app_install.name,
                                app_install.os,
                                app_install.status.id,
                                app_install.user_id,
                                app_install.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addAppInstall',
                            text:
                                'INSERT INTO "APP_INSTALL"( ' +
                                '  "EXTERNAL_INSTALL_ID", ' +
                                '  "EXTERNAL_NOTIFICATION_SERVICE_ID", ' +
                                '  "CREATED", ' +
                                '  "NAME", ' +
                                '  "OS", ' +
                                '  "STATUS_ID", ' +
                                '  "USER_ID" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6, $7 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                app_install.external_install_id,
                                app_install.external_notification_service_id,
                                app_install.created,
                                app_install.name,
                                app_install.os,
                                app_install.status.id,
                                app_install.user_id
                            ]
                        }, function(err, result) {
                            if (!err) {
                                app_install.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete an app_install
         *
         * @name deleteAppInstall
         * @memberof datastore/postgres
         * @function
         * @public
         * @param app_install {Object} The app_install to delete
         * @param callback {Function} Callback to fire when finished
         */
        deleteAppInstall: function(app_install, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteAppInstall',
                        text:
                            'DELETE FROM ' +
                            '  "APP_INSTALL" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            app_install.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a HPC package list
         *
         * @name addOrUpdateHpcPackages
         * @memberof datastore.postgres
         * @function
         * @public
         * @param packages {object} The package list object to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateHpcPackages: function(packages, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    packages.updated = new Date();

                    // Are we updating or inserting?
                    if (packages.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHpcPackages',
                            text:
                                'UPDATE ' +
                                '  "HPC_PACKAGES" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "UPDATED" = $2, '+
                                '  "PACKAGES" = $3 ' +
                                'WHERE ' +
                                '  "ID" = $4 ',
                            values: [
                                packages.hpc_id,
                                packages.updated,
                                JSON.stringify(packages.packages),
                                packages.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHpcPackages',
                            text:
                                'INSERT INTO "HPC_PACKAGES"( ' +
                                '  "HPC_ID", ' +
                                '  "UPDATED", ' +
                                '  "PACKAGES" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID"',
                            values: [
                                packages.hpc_id,
                                packages.updated,
                                JSON.stringify(packages.packages)
                            ]
                        }, function(err, result) {
                            if (!err) {
                                packages.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a HPC package list installed on the supplied HPC
         *
         * @name findHpcPackagesForHPC
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcPackagesForHPC: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcPackagesForHPC',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "HPC_PACKAGES" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        var packages = result && result.rows ? result.rows[0] : null;
                        callback(err, packages);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a HPC schedule
         *
         * @name addOrUpdateHpcSchedule
         * @memberof datastore.postgres
         * @function
         * @public
         * @param schedule {object} The package list object to save
         * @param callback Callback to fire when finished
         */
        addOrUpdateHpcSchedule: function(schedule, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    schedule.updated = new Date();
                    // Are we updating or inserting?
                    if (schedule.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHpcSchedule',
                            text:
                                'UPDATE ' +
                                '  "HPC_SCHEDULE" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "START_TIME" = $2, '+
                                '  "END_TIME" = $3, ' +
                                '  "UPDATED" = $4 ' +
                                'WHERE ' +
                                '  "ID" = $5 ',
                            values: [
                                schedule.hpc_id,
                                schedule.start_time,
                                schedule.end_time,
                                schedule.updated,
                                schedule.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHpcSchedule',
                            text:
                                'INSERT INTO "HPC_SCHEDULE"( ' +
                                '  "HPC_ID", ' +
                                '  "START_TIME", ' +
                                '  "END_TIME", ' +
                                '  "UPDATED" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                schedule.hpc_id,
                                schedule.start_time,
                                schedule.end_time,
                                schedule.updated
                            ]
                        }, function(err, result) {
                            if (!err) {
                                schedule.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a HPC schedule for the supplied HPC
         *
         * @name findHpcScheduleForHPC
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcScheduleForHPC: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcScheduleForHPC',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "HPC_SCHEDULE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        var schedule = result && result.rows ? result.rows[0] : null;
                        callback(err, schedule);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a home automation device
         *
         * @name addOrUpdateHaDevice
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {object} The home automation device to save
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateHaDevice: function(had, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (had.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHaDevice',
                            text:
                                'UPDATE ' +
                                '  "HOME_AUTOMATION_DEVICE" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "NAME" = $2, '+
                                '  "ICON_ID" = $3, ' +
                                '  "DEVICE_ID" = $4, ' +
                                '  "DATA" = $5, ' +
                                '  "SHORTCUT" = $6 ' +
                                'WHERE ' +
                                '  "ID" = $7 ',
                            values: [
                                had.hpc_id,
                                had.name,
                                had.icon.id,
                                had.device.id,
                                had.data,
                                had.shortcut,
                                had.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHaDevice',
                            text:
                                'INSERT INTO "HOME_AUTOMATION_DEVICE"( ' +
                                '  "HPC_ID", ' +
                                '  "NAME", ' +
                                '  "ICON_ID", ' +
                                '  "DEVICE_ID", ' +
                                '  "DATA", ' +
                                '  "EXTERNAL_ID" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, ' +
                                '  (SELECT "md5"(last_value || \'\') FROM "HOME_AUTOMATION_DEVICE_ID_seq") ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID", ' +
                                '  "EXTERNAL_ID" ',
                            values: [
                                had.hpc_id,
                                had.name,
                                had.icon.id,
                                had.device.id,
                                had.data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                had.id          = result.rows[0].ID;
                                had.external_id = result.rows[0].EXTERNAL_ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Save the data field of a home automation device
         *
         * @name saveHaDeviceData
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {object} The home automation device to save
         * @param callback {Function} A callback to fire when finished
         */
        saveHaDeviceData: function(had, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'saveHaDeviceData',
                        text:
                            'UPDATE ' +
                            '  "HOME_AUTOMATION_DEVICE" ' +
                            'SET ' +
                            '  "DATA" = $1 ' +
                            'WHERE ' +
                            '  "ID" = $2 ',
                        values: [
                            had.data,
                            had.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a home automation device
         *
         * @name deleteHaDevice
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {object} The home automation device to delete
         * @param callback {Function} A callback with the usual error and result parameters
         */
        deleteHaDevice: function(had, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteHaDevice',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            had.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HA devices controlled by the supplied HPC
         *
         * @name findHpcHaDevicesByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcHaDevicesByHpcID: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcHaDevicesByHpcID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            'ORDER BY ' +
                            '  "CREATED" DESC ',
                        values: [hpc_id]
                    }, function(err, result) {
                        done();

                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for HA devices used as the whole home meter, for the supplied HPC.
         *
         * Normally, this should yield only one or no results, however the
         * database does not enforce this, so it is up to the application logic
         * to check this.
         *
         * @name findWholeHomeMeterHaDevicesByHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findWholeHomeMeterHaDevicesByHpcId: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findWholeHomeMeterHaDevicesByHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            '  AND (' +
                            '    ("DATA"->>\'whole_home_source\')::boolean is TRUE OR ' +
                                // replace_smart_meter is a legacy flag and may be removed eventually,
                                // (make sure in Postgres it's renamed into whole_home_source)
                            '    ("DATA"->>\'replace_smart_meter\')::boolean is TRUE' +
                            '  ) ' +
                            'ORDER BY ' +
                            '  "CREATED" DESC ',
                        values: [hpc_id]
                    }, function(err, result) {
                        done();
                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for HA devices used as the solar meter, for the supplied HPC.
         *
         * Normally, this should yield only one or no results, however the
         * database does not enforce this, so it is up to the application logic
         * to check this.
         *
         * @name findSolarMeterHaDevicesByHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findSolarMeterHaDevicesByHpcId: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSolarMeterHaDevicesByHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            '  AND ("DATA"->>\'solar_source\')::boolean is TRUE ' +
                            'ORDER BY ' +
                            '  "CREATED" DESC ',
                        values: [hpc_id]
                    }, function(err, result) {
                        done();
                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaDevices in the given HaRoom and controlled by the supplied HPC
         *
         * @name findHaDevicesByHaRoomIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har_id {Number} A private identifier of the HaRoom
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaDevicesByHaRoomIdAndHpcId: function(har_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDevicesByHaRoomIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_DEVICE".*, "HOME_AUTOMATION_DEVICE_ROOM"."ORDINAL_INFO"  ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            '  INNER JOIN "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            '    ON "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_DEVICE_ID" = "V_HOME_AUTOMATION_DEVICE"."ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_ROOM_ID" = $2 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."CREATED" DESC ',
                        values: [
                            hpc_id,
                            har_id
                        ]
                    }, function(err, result) {
                        done();

                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaDevices in the given HaRoom,
         * having the given Transport Type and controlled by the supplied HPC
         *
         * @name findHaDevicesByHaRoomIdTransportTypeAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har_id {Number} A private identifier of the HaRoom
         * @param comparator {String} A comparator used with the Transport Type (i.e. '=', '<>')
         * @param transport_type {String} A Transport Type name
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaDevicesByHaRoomIdTransportTypeAndHpcId: function(har_id, comparator, transport_type, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        // since comparator is directly included in the query text below (not in a list of values),
                        // the name has to be unique to avoid caching issues
                        // (e.g.: = 'ZigBee HA' and <> 'ZigBee HA' would return identical results)
                        name: 'findHaDevicesByHaRoomIdTransportTypeAndHpcId' + comparator,
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_DEVICE".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            '  INNER JOIN "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            '    ON "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_DEVICE_ID" = "V_HOME_AUTOMATION_DEVICE"."ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."HPC_ID" = $1 AND ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."TRANSPORT_TYPE_NAME" ' + comparator + ' $2 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_ROOM_ID" = $3 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."CREATED" DESC ',
                        values: [
                            hpc_id,
                            transport_type,
                            har_id
                        ]
                    }, function(err, result) {
                        done();

                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaDevices in the given HaZone and controlled by the supplied HPC
         *
         * @name findHaDevicesByHaZoneIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz_id {Number} A private identifier of the HaZone
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaDevicesByHaZoneIdAndHpcId: function(haz_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDevicesByHaZoneIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_DEVICE".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            '  INNER JOIN "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            '    ON "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_DEVICE_ID" = "V_HOME_AUTOMATION_DEVICE"."ID" ' +
                            '  INNER JOIN "HOME_AUTOMATION_ROOM_ZONE" ' +
                            '    ON "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ROOM_ID" = "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_ROOM_ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ZONE_ID" = $2 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."CREATED" DESC ',
                        values: [
                            hpc_id,
                            haz_id
                        ]
                    }, function(err, result) {
                        done();

                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaDevices in the given HaZone,
         * having the given Transport Type and controlled by the supplied HPC
         *
         * @name findHaDevicesByHaZoneIdTransportTypeAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz_id {Number} A private identifier of the HaZone
         * @param comparator {String} A comparator used with the Transport Type (i.e. '=', '<>')
         * @param transport_type {String} A Transport Type name
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaDevicesByHaZoneIdTransportTypeAndHpcId: function(haz_id, comparator, transport_type, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDevicesByHaZoneIdTransportTypeAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_DEVICE".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            '  INNER JOIN "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            '    ON "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_DEVICE_ID" = "V_HOME_AUTOMATION_DEVICE"."ID" ' +
                            '  INNER JOIN "HOME_AUTOMATION_ROOM_ZONE" ' +
                            '    ON "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ROOM_ID" = "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_ROOM_ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."HPC_ID" = $1 AND ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."TRANSPORT_TYPE_NAME" ' + comparator + ' $2 AND ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ZONE_ID" = $3 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_DEVICE"."CREATED" DESC ',
                        values: [
                            hpc_id,
                            transport_type,
                            haz_id
                        ]
                    }, function(err, result) {
                        done();

                        var hads = result && result.rows ? result.rows : [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HA device with the specified external identifier and private identifier
         *
         * @name findHpcHaDevicesByExternalIDHpcID
         * @memberof datastore
         * @function
         * @public
         * @param external_id {String} the external identifier of the HPC
         * @param hpc_id {Number} the private identifier of the HPC
         * @param callback {Function} a callback with the usual error and result parameters
         */
        findHpcHaDevicesByExternalIDHpcID: function(external_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcHaDevicesByExternalIDHpcID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "EXTERNAL_ID" = $2 ',
                        values: [
                            hpc_id,
                            external_id
                        ]
                    }, function(err, result) {
                        done();
                        var had = result && result.rows ? result.rows[0] : null;
                        callback(err, had);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HA device with the specified device type, external identifier and private identifier
         *
         * @name findHaDeviceByTypeExternalIdForHpc
         * @memberof datastore
         * @function
         * @public
         * @param device_type {String} the device type
         * @param external_id {String} the external identifier of the HA device
         * @param hpc_id {Number} the private identifier of the HPC
         * @param callback {Function} a callback with the usual error and result parameters
         */
        findHaDeviceByTypeExternalIdForHpc: function(device_type, external_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDeviceByTypeExternalIdForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "EXTERNAL_ID" = $2 AND ' +
                            '  "DEVICE_TYPE_TAG" = $3 ',
                        values: [
                            hpc_id,
                            external_id,
                            device_type
                        ]
                    }, function(err, result) {
                        done();
                        var had = result && result.rows ? result.rows[0] : null;
                        callback(err, had);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search HA devices with the specified device type for the given HPC.
         *
         * @name findHaDevicesByTypeForHpc
         * @memberof datastore
         * @function
         * @public
         * @param device_type {String} the device type ("tag")
         * @param hpc_id {Number} the private identifier of the HPC
         * @param callback {Function} a callback with the error and result parameters
         *                            The result is an array of hads.
         */
        findHaDevicesByTypeForHpc: function(device_type, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDevicesByTypeForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "DEVICE_TYPE_TAG" = $2 ',
                        values: [
                            hpc_id,
                            device_type
                        ]
                    }, function(err, result) {
                        done();
                        var hads = (result && result.rows) || [];
                        callback(err, hads);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HA device (Intesis Box) with the specified Name and HPC ID
         *
         * @name findHaDeviceByIntesisNameForHPC
         * @memberof datastore
         * @function
         * @public
         * @param name {String} A unique name of the Intesis Box (e.g. 'WMP_8096E8')
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} a callback with the usual error and result parameters
         */
        findHaDeviceByIntesisNameForHPC: function(name, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDeviceByIntesisNameForHPC',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "TRANSPORT_DATA"::json->>\'name\' = $2 AND ' +
                            '  "TRANSPORT_TYPE_NAME" = $3 ',
                        values: [
                            hpc_id,
                            name,
                            'Intesis'
                        ]
                    }, function(err, result) {
                        done();
                        var had = result && result.rows ? result.rows[0] : null;
                        callback(err, had);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HA device with the specified IEEE, Network Address and HPC identifier
         *
         * @name findHaDeviceByIeeeNwkForHpc
         * @memberof datastore
         * @function
         * @public
         * @param ieee {String} the IEEE of the HA Device
         * @param nwk {String} the Network Address of the HA Device
         * @param hpc_id {Number} the private identifier of the HPC
         * @param callback {Function} a callback with the usual error and result parameters
         */
        findHaDeviceByIeeeNwkForHpc: function(ieee, nwk, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDeviceByIeeeNwkForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ( ' +
                            '    ("TRANSPORT_DATA"::json->>\'ieee\' = $2 AND $2 <> $3) OR ' +
                            '    ("TRANSPORT_DATA"::json->>\'nwk\' = $4 AND ($2 = $3 OR "TRANSPORT_DATA"::json->>\'ieee\' = $3)) ' +
                            '  ) AND ' +
                            '  "TRANSPORT_TYPE_NAME" = $5 ',
                        values: [
                            hpc_id,
                            ieee,
                            'FFFFFFFFFFFFFFFF',
                            nwk,
                            'ZigBee HA'
                        ]
                    }, function(err, result) {
                        done();
                        var had = result && result.rows ? result.rows[0] : null;
                        callback(err, had);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the full HA device info with the specified HPC and HA device identifiers
         *
         * @name findHaDeviceInfo
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} the private identifier of the HPC
         * @param had_external_id {String} An external identifier of the HA device
         * @param callback {Function} a callback with the usual error and result parameters
         */
        findHaDeviceInfo: function(hpc_id, had_external_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaDeviceInfo',
                        text:
                            'SELECT ' +
                            '  "DEVICE_INFO"."DATA" ' +
                            'FROM ' +
                            '  "HOME_AUTOMATION_DEVICE" ' +
                            '  JOIN "DEVICE" ON "DEVICE"."ID" = "HOME_AUTOMATION_DEVICE"."DEVICE_ID" ' +
                            '  JOIN "DEVICE_INFO" ON "DEVICE_INFO"."ID" = "DEVICE"."DEVICE_INFO_ID" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "EXTERNAL_ID" = $2 ',
                        values: [
                            hpc_id,
                            had_external_id
                        ]
                    }, function(err, result) {
                        done();
                        var data = ((result && result.rows ? result.rows[0] : null) || {}).DATA;
                        callback(err, data);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaRooms controlled by the supplied HPC
         *
         * @name findHaRoomsByHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaRoomsByHpcId: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaRoomsByHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ROOM" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            'ORDER BY ' +
                            '  "NAME" ASC ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var hars = result && result.rows ? result.rows : [];
                        callback(err, hars);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find 'All Lighting' room for specific HPC
         *
         * @name findAllLightingRoomByHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAllLightingRoomByHpcId: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAllLightingRoomByHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ROOM" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND' +
                            '  ("DATA"::json->>\'all_lighting_room\')::boolean is true',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();
                        // Return first found All lighting room from the results or else null
                        var har = result && result.rows ? result.rows[0] : null;
                        callback(err, har);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaRooms accommodating the given HaDevice and controlled by the supplied HPC
         *
         * @name findHaRoomsByHaDeviceIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had_id {Number} A private identifier of the HaDevice
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaRoomsByHaDeviceIdAndHpcId: function(had_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaRoomsByHaDeviceIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_ROOM".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ROOM" ' +
                            '  INNER JOIN "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            '    ON "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_ROOM_ID" = "V_HOME_AUTOMATION_ROOM"."ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_ROOM"."HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_ROOM"."HOME_AUTOMATION_DEVICE_ID" = $2 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_ROOM"."NAME" ASC ',
                        values: [
                            hpc_id,
                            had_id
                        ]
                    }, function(err, result) {
                        done();

                        var hars = result && result.rows ? result.rows : [];
                        callback(err, hars);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaRooms in the given HaZone and controlled by the supplied HPC
         *
         * @name findHaRoomsByHaZoneIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz_id {Number} A private identifier of the HaZone
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaRoomsByHaZoneIdAndHpcId: function(haz_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaRoomsByHaZoneIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_ROOM".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ROOM" ' +
                            '  INNER JOIN "HOME_AUTOMATION_ROOM_ZONE" ' +
                            '    ON "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ROOM_ID" = "V_HOME_AUTOMATION_ROOM"."ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_ROOM"."HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ZONE_ID" = $2 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_ROOM"."NAME" ASC ',
                        values: [
                            hpc_id,
                            haz_id
                        ]
                    }, function(err, result) {
                        done();

                        var hars = result && result.rows ? result.rows : [];
                        callback(err, hars);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HaRoom with the given external identifier and the private HPC identifier
         *
         * @name findHaRoomByExternalIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param external_id {String} An external identifier of the HARoom
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaRoomByExternalIdAndHpcId: function(external_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaRoomByExternalIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ROOM" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "EXTERNAL_ID" = $2 ',
                        values: [
                            hpc_id,
                            external_id
                        ]
                    }, function(err, result) {
                        done();
                        var har = result && result.rows ? result.rows[0] : null;
                        callback(err, har);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a home automation room
         *
         * @name addOrUpdateHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to save
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateHaRoom: function(har, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (har.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHaRoom',
                            text:
                                'UPDATE ' +
                                '  "HOME_AUTOMATION_ROOM" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "ICON_ID" = $2, ' +
                                '  "NAME" = $3, '+
                                '  "DATA" = $4, ' +
                                '  "SHORTCUT" = $5 ' +
                                'WHERE ' +
                                '  "ID" = $6 ',
                            values: [
                                har.hpc_id,
                                har.icon.id,
                                har.name,
                                har.data,
                                har.shortcut,
                                har.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHaRoom',
                            text:
                                'INSERT INTO "HOME_AUTOMATION_ROOM"( ' +
                                '  "EXTERNAL_ID", ' +
                                '  "HPC_ID", ' +
                                '  "ICON_ID", ' +
                                '  "NAME", ' +
                                '  "DATA", ' +
                                '  "EDITABLE" ' +
                                ') VALUES ( ' +
                                '  (SELECT "md5"(last_value::text) FROM "HOME_AUTOMATION_ROOM_ID_seq"), ' +
                                '  $1, $2, $3, $4, $5 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID", ' +
                                '  "EXTERNAL_ID" ',
                            values: [
                                har.hpc_id,
                                har.icon.id,
                                har.name,
                                har.data,
                                har.editable
                            ]
                        }, function(err, result) {
                            if (!err) {
                                har.id          = result.rows[0].ID;
                                har.external_id = result.rows[0].EXTERNAL_ID;
                            }
                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a home automation room
         *
         * @name deleteHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to delete
         * @param callback {Function} A callback with the usual error and result parameters
         */
        deleteHaRoom: function(har, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteHaRoom',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_ROOM" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            har.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Add a home automation device into a home automation room
         *
         * @name addHaDeviceToHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {Object} The home automation device to add
         * @param har {Object} The home automation room to add to
         * @param ordinal_info {Object} Ordinal information to be added. Consists of:
         *          group {Object} Group consists of:
         *              ordinal        {Number} Group ordinal
         *              device_ordinal {Number} Device ordinal within group
         * @param callback {Function} A callback to fire when finished
         */
        addHaDeviceToHaRoom: function(had, har, ordinal_info, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'addHaDeviceToHaRoom',
                        text:
                            'INSERT INTO "HOME_AUTOMATION_DEVICE_ROOM"( ' +
                            '  "HOME_AUTOMATION_DEVICE_ID", ' +
                            '  "HOME_AUTOMATION_ROOM_ID", ' +
                            '  "ORDINAL_INFO" ' +
                            ') VALUES ( ' +
                            '  $1, $2, $3 ' +
                            ') ',
                        values: [
                            had.id,
                            har.id,
                            ordinal_info
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Update HA device ordinal information for HA room
         *
         * @name updateHaDeviceOrdinalForHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had_id {String} HA device identifier
         * @param har_id {String} HA room identifier
         * @param ordinal_info {Object} Ordinal information to be updated. Consists of:
         *          group {Object} Group consists of:
         *              ordinal        {Number} Group ordinal
         *              device_ordinal {Number} Device ordinal within group
         * @param callback {Function} A callback to fire when finished
         */
        updateHaDeviceOrdinalForHaRoom: function(had_id, har_id, ordinal_info, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'updateHaDeviceOrdinalForHaRoom',
                        text:
                        'UPDATE "HOME_AUTOMATION_DEVICE_ROOM" ' +
                        'SET ' +
                        '  "ORDINAL_INFO" = $3 ' +
                        'WHERE ' +
                        '  "HOME_AUTOMATION_DEVICE_ID" = $1 AND ' +
                        '  "HOME_AUTOMATION_ROOM_ID" = $2 ',
                        values: [
                            had_id,
                            har_id,
                            ordinal_info
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove a home automation device from a home automation room
         *
         * @name removeHaDeviceFromHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {Object} The home automation device to remove
         * @param har {Object} The home automation room to remove from
         * @param callback {Function} A callback to fire when finished
         */
        removeHaDeviceFromHaRoom: function(had, har, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaDeviceFromHaRoom',
                        text:
                            'DELETE FROM ' +
                                '  "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_DEVICE_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_ROOM_ID" = $2 ',
                        values: [
                            had.id,
                            har.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove a home automation device from all home automation rooms
         *
         * @name removeHaDeviceFromHaRooms
         * @memberof datastore.postgres
         * @function
         * @public
         * @param had {Object} The home automation device to remove
         * @param callback {Function} A callback to fire when finished
         */
        removeHaDeviceFromHaRooms: function(had, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaDeviceFromHaRooms',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_DEVICE_ID" = $1 ',
                        values: [
                            had.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove all home automation devices from a home automation room
         *
         * @name removeHaDevicesFromHaRoom
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to remove from
         * @param callback {Function} A callback to fire when finished
         */
        removeHaDevicesFromHaRoom: function(har, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaDevicesFromHaRoom',
                        text:
                            'DELETE FROM ' +
                                '  "HOME_AUTOMATION_DEVICE_ROOM" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_ROOM_ID" = $1 ',
                        values: [
                            har.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaZones controlled by the supplied HPC
         *
         * @name findHaZonesByHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaZonesByHpcId: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaZonesByHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ZONE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            'ORDER BY ' +
                            '  "NAME" ASC ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var hazs = result && result.rows ? result.rows : [];
                        callback(err, hazs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the set of HaZones accommodating the given HaRoom and controlled by the supplied HPC
         *
         * @name findHaZonesByHaRoomIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har_id {Number} A private identifier of the HaRoom
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaZonesByHaRoomIdAndHpcId: function(har_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaZonesByHaRoomIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  "V_HOME_AUTOMATION_ZONE".* ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ZONE" ' +
                            '  INNER JOIN "HOME_AUTOMATION_ROOM_ZONE" ' +
                            '    ON "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ZONE_ID" = "V_HOME_AUTOMATION_ZONE"."ID" ' +
                            'WHERE ' +
                            '  "V_HOME_AUTOMATION_ZONE"."HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE"."HOME_AUTOMATION_ROOM_ID" = $2 ' +
                            'ORDER BY ' +
                            '  "V_HOME_AUTOMATION_ZONE"."NAME" ASC ',
                        values: [
                            hpc_id,
                            har_id
                        ]
                    }, function(err, result) {
                        done();

                        var hazs = result && result.rows ? result.rows : [];
                        callback(err, hazs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for the HaZone with the given external identifier and the private HPC identifier
         *
         * @name findHaZoneByExternalIdAndHpcId
         * @memberof datastore.postgres
         * @function
         * @public
         * @param external_id {String} An external identifier of the HAZone
         * @param hpc_id {Number} A private identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHaZoneByExternalIdAndHpcId: function(external_id, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHaZoneByExternalIdAndHpcId',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_ZONE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "EXTERNAL_ID" = $2 ',
                        values: [
                            hpc_id,
                            external_id
                        ]
                    }, function(err, result) {
                        done();
                        var haz = result && result.rows ? result.rows[0] : null;
                        callback(err, haz);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a home automation zone
         *
         * @name addOrUpdateHaZone
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz {Object} The home automation zone to save
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateHaZone: function(haz, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (haz.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateHaZone',
                            text:
                                'UPDATE ' +
                                '  "HOME_AUTOMATION_ZONE" ' +
                                'SET ' +
                                '  "HPC_ID" = $1, ' +
                                '  "ICON_ID" = $2, ' +
                                '  "NAME" = $3, '+
                                '  "DATA" = $4, ' +
                                '  "SHORTCUT" = $5 ' +
                                'WHERE ' +
                                '  "ID" = $6 ',
                            values: [
                                haz.hpc_id,
                                haz.icon.id,
                                haz.name,
                                haz.data,
                                haz.shortcut,
                                haz.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addHaZone',
                            text:
                                'INSERT INTO "HOME_AUTOMATION_ZONE"( ' +
                                '  "EXTERNAL_ID", ' +
                                '  "HPC_ID", ' +
                                '  "ICON_ID", ' +
                                '  "NAME", ' +
                                '  "DATA" ' +
                                ') VALUES ( ' +
                                '  (SELECT "md5"(last_value::text) FROM "HOME_AUTOMATION_ZONE_ID_seq"), ' +
                                '  $1, $2, $3, $4 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID", ' +
                                '  "EXTERNAL_ID" ',
                            values: [
                                haz.hpc_id,
                                haz.icon.id,
                                haz.name,
                                haz.data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                haz.id          = result.rows[0].ID;
                                haz.external_id = result.rows[0].EXTERNAL_ID;
                            }
                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a home automation zone
         *
         * @name deleteHaZone
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz {Object} The home automation zone to delete
         * @param callback {Function} A callback with the usual error and result parameters
         */
        deleteHaZone: function(haz, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteHaZone',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_ZONE" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            haz.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Add a home automation room into a home automation zone
         *
         * @name addHaRoomToHaZone
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to add
         * @param haz {Object} The home automation zone to add to
         * @param callback {Function} A callback to fire when finished
         */
        addHaRoomToHaZone: function(har, haz, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'addHaRoomToHaZone',
                        text:
                            'INSERT INTO "HOME_AUTOMATION_ROOM_ZONE"( ' +
                            '  "HOME_AUTOMATION_ROOM_ID", ' +
                            '  "HOME_AUTOMATION_ZONE_ID" ' +
                            ') VALUES ( ' +
                            '  $1, $2 ' +
                            ') ',
                        values: [
                            har.id,
                            haz.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove a home automation room from a home automation zone
         *
         * @name removeHaRoomFromHaZone
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to remove
         * @param haz {Object} The home automation zone to remove from
         * @param callback {Function} A callback to fire when finished
         */
        removeHaRoomFromHaZone: function(har, haz, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaRoomFromHaZone',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_ROOM_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_ZONE_ID" = $2 ',
                        values: [
                            har.id,
                            haz.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove a home automation room from all home automation zones
         *
         * @name removeHaRoomFromHaZones
         * @memberof datastore.postgres
         * @function
         * @public
         * @param har {Object} The home automation room to remove
         * @param callback {Function} A callback to fire when finished
         */
        removeHaRoomFromHaZones: function(har, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaRoomFromHaZones',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_ROOM_ID" = $1 ',
                        values: [
                            har.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Remove all home automation rooms from a home automation zone
         *
         * @name removeHaRoomsFromHaZone
         * @memberof datastore.postgres
         * @function
         * @public
         * @param haz {Object} The home automation zone to remove from
         * @param callback {Function} A callback to fire when finished
         */
        removeHaRoomsFromHaZone: function(haz, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeHaRoomsFromHaZone',
                        text:
                            'DELETE FROM ' +
                            '  "HOME_AUTOMATION_ROOM_ZONE" ' +
                            'WHERE ' +
                            '  "HOME_AUTOMATION_ZONE_ID" = $1 ',
                        values: [
                            haz.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for audit log entries attached to the specified HPC
         *
         * @name findAuditLogByHpcID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param priority {Number} The lowest priority of results to retrieve
         * @param limit {Number} The maximum number of results to retrieve
         * @param offset {Number} The offset of the results to retrieve
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAuditLogByHpcID: function(hpc_id, priority, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "AUDIT_LOG".*, ' +
                        '  "USER"."USERNAME" ' +
                        'FROM ' +
                        '  "AUDIT_LOG" ' +
                        '  INNER JOIN "USER" ON "USER"."ID" = "AUDIT_LOG"."ORIGINATING_USER_ID" '+
                        'WHERE ' +
                        '  "AUDIT_LOG"."HPC_ID" = $1 AND ' +
                        '  "AUDIT_LOG"."PRIORITY" >= $2 ' +
                        'ORDER BY ' +
                        '  "AUDIT_LOG"."TIMESTAMP" DESC ';
                    var params = [ hpc_id, priority ];
                    if (limit) {
                        sql +=
                            'OFFSET ' +
                            '  $3 ' +
                            'LIMIT ' +
                            '  $4 ';
                        params.push(offset, limit);
                    }

                    clientQuery(client, false, {
                        name: 'findAuditLogByHpcID' + (limit ? '_withLimit': ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();

                        var srs = result && result.rows ? result.rows : [];
                        callback(err, srs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for audit log entries attached to the specified User
         *
         * @name findAuditLogByUserID
         * @memberof datastore.postgres
         * @function
         * @public
         * @param user_id {Number} The private identifier of the user
         * @param priority {Number} The lowest priority of results to retrieve
         * @param limit {Number} The maximum number of results to retrieve
         * @param offset {Number} The offset of the results to retrieve
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findAuditLogByUserID: function(user_id, priority, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT ' +
                        '  "AUDIT_LOG".*, ' +
                        '  "USER"."USERNAME" ' +
                        'FROM ' +
                        '  "AUDIT_LOG" ' +
                        '  INNER JOIN "USER" ON ' +
                        '     ("USER"."ID" = "AUDIT_LOG"."ORIGINATING_USER_ID" OR ' +
                        '      "USER"."ID" = "AUDIT_LOG"."USER_ID") '+
                        'WHERE ' +
                        '  "AUDIT_LOG"."USER_ID" = $1 AND ' +
                        '  "AUDIT_LOG"."PRIORITY" >= $2 ' +
                        'ORDER BY ' +
                        '  "AUDIT_LOG"."TIMESTAMP" DESC ';
                    var params = [ user_id, priority ];
                    if (limit) {
                        sql +=
                            'OFFSET ' +
                            '  $3 ' +
                            'LIMIT ' +
                            '  $4 ';
                        params.push(offset, limit);
                    }

                    clientQuery(client, false, {
                        name: 'findAuditLogByUserID' + (limit ? '_withLimit': ''),
                        text: sql,
                        values: params,
                    }, function(err, result) {
                        done();

                        var srs = result && result.rows ? result.rows : [];
                        callback(err, srs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert an audit log entry
         *
         * @name addAuditLog
         * @memberof datastore.postgres
         * @function
         * @public
         * @param audit_log {object} The audit log entry to insert
         * @param callback Callback to fire when finished
         */
        addAuditLog: function(audit_log, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (audit_log.id) {
                        callback('Cannot update audit log entry');
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addAuditLog',
                            text:
                                'INSERT INTO "AUDIT_LOG"( ' +
                                '  "HPC_ID", ' +
                                '  "USER_ID", ' +
                                '  "ORIGINATING_USER_ID", ' +
                                '  "ORIGINATING_IP", ' +
                                '  "PRIORITY", ' +
                                '  "MESSAGE" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                audit_log.hpc_id,
                                audit_log.user_id,
                                audit_log.originating_user_id,
                                audit_log.originating_ip,
                                audit_log.priority,
                                audit_log.message
                            ]
                        }, function(err, result) {
                            if (!err) {
                                audit_log.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a user's audit log entries
         *
         * @name deleteAuditLogsForUser
         * @memberof datastore
         * @function
         * @public
         * @param user_id {number} The identifier of the user that will have their audit logs deleted
         * @param callback {Function} A callback to fire when finished
         */
        deleteAuditLogsForUser: function(user_id, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteAuditLogsForUser',
                        text:
                            'DELETE FROM ' +
                            '  "AUDIT_LOG" ' +
                            'WHERE ' +
                            '  "USER_ID" = $1 OR ' +
                            '  "ORIGINATING_USER_ID" = $1 ',
                        values: [
                            user_id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Insert/update a recurring job record
         *
         * @name addOrUpdateRecurringJob
         * @memberof datastore
         * @function
         * @public
         * @param recurring_job {object} The recurring job record to insert/update
         * @param callback Callback to fire when finished
         */
        addOrUpdateRecurringJob: function(recurring_job, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (recurring_job.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updatedRecurringJob',
                            text:
                                'UPDATE ' +
                                '  "RECURRING_JOB" ' +
                                'SET ' +
                                '  "ACTIVE" = $1, '+
                                '  "LAST_SUCCESSFUL_EXECUTION" = $2, ' +
                                '  "FAILURE_COUNT" = $3, ' +
                                '  "NEXT_EXECUTION" = $4, ' +
                                '  "JOB_HANDLER" = $5, ' +
                                '  "JOB_DATA" = $6 ' +
                                'WHERE ' +
                                '  "ID" = $7 ',
                            values: [
                                recurring_job.active,
                                recurring_job.last_successful_execution,
                                recurring_job.failure_count,
                                recurring_job.next_execution,
                                recurring_job.job_handler,
                                recurring_job.job_data,
                                recurring_job.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addRecurringJob',
                            text:
                                'INSERT INTO "RECURRING_JOB"( ' +
                                '  "ACTIVE", ' +
                                '  "LAST_SUCCESSFUL_EXECUTION", ' +
                                '  "FAILURE_COUNT", ' +
                                '  "NEXT_EXECUTION", ' +
                                '  "JOB_HANDLER", ' +
                                '  "JOB_DATA" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4, $5, $6 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                recurring_job.active,
                                recurring_job.last_successful_execution,
                                recurring_job.failure_count,
                                recurring_job.next_execution,
                                recurring_job.job_handler,
                                recurring_job.job_data
                            ]
                        }, function(err, result) {
                            if (!err) {
                                recurring_job.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a recurring job record
         *
         * @name deleteAppInstall
         * @memberof datastore
         * @function
         * @public
         * @param recurring_job {Object} The recurring job record to delete
         * @param callback {Function} A callback to fire when finished
         */
        deleteRecurringJob: function(recurring_job, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteRecurringJob',
                        text:
                            'DELETE FROM ' +
                            '  "RECURRING_JOB" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            recurring_job.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for recurring job records attached to the specified HPC
         *
         * @name findRecurringJobsByHpcSerial
         * @memberof datastore
         * @function
         * @public
         * @param hpc_serial {String} The public identifier of the HPC
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findRecurringJobsByHpcSerial: function(hpc_serial, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findRecurringJobsByHpcSerial',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RECURRING_JOB" ' +
                            'WHERE ' +
                            '  "JOB_DATA"->>\'hpc\' = $1 ',
                        values: [
                            hpc_serial
                        ]
                    }, function(err, result) {
                        done();

                        var jobs = result && result.rows ? result.rows : [];
                        callback(err, jobs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for HPC serials that have an active recurring job of the specified type
         *
         * @name findHpcSerialsForRecurringJobs
         * @memberof datastore
         * @function
         * @public
         * @param handler {String} The name of the job handler
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findHpcSerialsForRecurringJobs: function(handler, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcSerialsForRecurringJobs',
                        text:
                            'SELECT ' +
                            '  "JOB_DATA"->>\'hpc\' AS hpc_id ' +
                            'FROM ' +
                            '  "RECURRING_JOB" ' +
                            'WHERE ' +
                            '  "JOB_HANDLER" = $1 AND ' +
                            '  "ACTIVE" = true ',
                        values: [
                            handler
                        ]
                    }, function(err, result) {
                        done();

                        var jobs = result && result.rows ? result.rows : [];
                        callback(err, jobs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for recurring job records that are ready for immediate execution
         *
         * @name findReadyRecurringJobs
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findReadyRecurringJobs: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findReadyRecurringJobs',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RECURRING_JOB" ' +
                            'WHERE ' +
                            '  "ACTIVE" = true AND ' +
                            '  ("NEXT_EXECUTION" IS NULL OR "NEXT_EXECUTION" < NOW()) ' +
                            'ORDER BY ' +
                            '  "NEXT_EXECUTION" ASC ',
                        values: []
                    }, function(err, result) {
                        done();

                        var jobs = result && result.rows ? result.rows : [];
                        callback(err, jobs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for a weather station with the supplied identifier
         *
         * @name findWeatherStationById
         * @memberof datastore
         * @function
         * @public
         * @param id {Integer} The identifier of the weather station
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findWeatherStationById: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findWeatherStationById',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_WEATHER_STATION" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();

                        var station = result && result.rows ? result.rows[0] : null;
                        callback(err, station);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for all weather stations near to the supplied lat/long point
         *
         * @name findWeatherStationsNearPoint
         * @memberof datastore
         * @function
         * @public
         * @param lat {Number} The latitude of the point to search nearby
         * @param long {Number} The longitude of the point to search nearby
         * @param radius {Number} The radial distance to search
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findWeatherStationsNearPoint: function(lat, long, radius, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findWeatherStationsNearPoint',
                        text:
                            'SELECT ' +
                            '  "V_WEATHER_STATION".*, ' +
                            '  ST_Distance_Sphere("V_WEATHER_STATION"."LATLONG", ST_MakePoint($1, $2)) AS "DISTANCE" ' +
                            'FROM ' +
                            '  "V_WEATHER_STATION" ' +
                            'WHERE ' +
                            '  ST_Distance_Sphere("V_WEATHER_STATION"."LATLONG", ST_MakePoint($1, $2)) <= $3 ' +
                            'ORDER BY ' +
                            ' "DISTANCE" DESC ' +
                            'LIMIT ' +
                            '  10 ',
                        values: [
                            lat,
                            long,
                            radius
                        ]
                    }, function(err, result) {
                        done();

                        var stations = result && result.rows ? result.rows : [];
                        callback(err, stations);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for active weather stations associated with the supplied HPC
         *
         * @name findWeatherStationsForHpc
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the Hpc that forms the basis of the search
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findWeatherStationsForHpc: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findWeatherStationsForHpc',
                        text:
                            'SELECT ' +
                            '  "V_WEATHER_STATION".*, ' +
                            '  "WEATHER_STATION_HPC"."DISTANCE" ' +
                            'FROM ' +
                            '  "V_WEATHER_STATION" ' +
                            '  INNER JOIN "WEATHER_STATION_HPC" ON "V_WEATHER_STATION"."ID" = "WEATHER_STATION_HPC"."WEATHER_STATION_ID" ' +
                            'WHERE ' +
                            '  "WEATHER_STATION_HPC"."HPC_ID" = $1 ' +
                            'ORDER BY ' +
                            '  "DISTANCE" DESC ' +
                            'LIMIT ' +
                            '  10 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        done();

                        var stations = result && result.rows ? result.rows : [];
                        callback(err, stations);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search an active weather station associated with the supplied HPC
         *
         * @name setWeatherStationForHPC
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the Hpc that will have its local weather station assigned
         * @param weather_station_id {Number} The private identifier of the weather station that will be assigned
         * @param distance {Number} The distance between the HPC and the weather station, in kilometers
         * @param callback {Function} A callback with the usual error and result parameters
         */
        setWeatherStationForHPC: function(hpc_id, weather_station_id, distance, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'setWeatherStationForHPC',
                        text:
                            'INSERT INTO "WEATHER_STATION_HPC"( ' +
                            '  "HPC_ID", ' +
                            '  "WEATHER_STATION_ID", ' +
                            '  "DISTANCE" ' +
                            ') VALUES ( ' +
                            '  $1, $2, $3 ' +
                            ') ',
                        values: [
                            hpc_id,
                            weather_station_id,
                            distance
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Disassociate all weather stations associated with the supplied HPC
         *
         * @name removeWeatherStationForHPC
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the Hpc that will have its local weather station assigned
         * @param callback {Function} A callback with the usual error and result parameters
         */
        removeWeatherStationForHPC: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'removeWeatherStationForHPC',
                        text:
                            'DELETE FROM ' +
                            '  "WEATHER_STATION_HPC" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find an externally sourced tip by its external identifier
         *
         * @name findExternalTipSourceByExternalID
         * @memberof datastore
         * @function
         * @public
         * @param external_id {String} The external identifier of the tip
         * @param callback {Function} A callback to fire when finished
         */
        findExternalTipSourceByExternalID: function(external_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findExternalTipSourceByExternalID',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "EXTERNAL_TIP_SOURCE" ' +
                            'WHERE ' +
                            '  "EXTERNAL_ID" = $1 ',
                        values: [
                            external_id
                        ]
                    }, function(err, result) {
                        done();

                        var tip = result && result.rows ? result.rows[0] : null;
                        callback(err, tip);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find HA devices that supply ground truth data for disaggregation
         *
         * @name findHaDevicesWithGroundTruth
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback to fire when finished
         */
        findHaDevicesWithGroundTruth: function(callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcHaDevicesWithGroundTruth',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "V_HOME_AUTOMATION_DEVICE" ' +
                            'WHERE ' +
                            '   EXISTS (' +
                            '       SELECT * FROM "GROUND_TRUTH_DEVICE" ' +
                            '       WHERE ' +
                            '           "HOME_AUTOMATION_DEVICE_EXTERNAL_ID" = "V_HOME_AUTOMATION_DEVICE"."EXTERNAL_ID" AND ' +
                            '           "HPC_ID" = "V_HOME_AUTOMATION_DEVICE"."HPC_ID" ' +
                            ')'
                    }, function(err, result) {
                        done();
                        var had = result && result.rows ? result.rows : [];
                        callback(err, had);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Create or edit a ground truth device entry
         *
         * @name addOrUpdateGroundTruth
         * @memberof datastore
         * @function
         * @public
         * @param ground_truth {Object} The ground truth object to create or update
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateGroundTruth: function(ground_truth, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (ground_truth.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateGroundTruth',
                            text:
                                'UPDATE ' +
                                '  "GROUND_TRUTH_DEVICE" ' +
                                'SET ' +
                                '  "NAME" = $1 '+
                                'WHERE ' +
                                '  "ID" = $2 ',
                            values: [
                                ground_truth.name,
                                ground_truth.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addGroundTruth',
                            text:
                                'INSERT INTO "GROUND_TRUTH_DEVICE"( ' +
                                '  "HPC_ID", ' +
                                '  "HOME_AUTOMATION_DEVICE_EXTERNAL_ID", ' +
                                '  "NAME", ' +
                                '  "ENDPOINT" ' +
                                ') VALUES ( ' +
                                '  $1, $2, $3, $4 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                ground_truth.hpc_id,
                                ground_truth.home_automation_device_external_id,
                                ground_truth.name,
                                ground_truth.endpoint
                            ]
                        }, function(err, result) {
                            if (!err) {
                                ground_truth.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find ground truth device entries for the given HA device
         *
         * @name findGroundTruthForHomeAutomationDevice
         * @memberof datastore
         * @function
         * @public
         * @param had {Object} The HA device
         * @param callback {Function} A callback to fire when finished
         */
        findGroundTruthForHomeAutomationDevice: function(had, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findGroundTruthForHomeAutomationDevice',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "GROUND_TRUTH_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_EXTERNAL_ID" = $2 ',
                        values: [
                            had.hpc_id,
                            had.external_id
                        ]
                    }, function(err, result) {
                        done();

                        var truths = result && result.rows ? result.rows : [];
                        callback(err, truths);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find the ground truth device entry for the given HA device endpoint
         *
         * @name findGroundTruthForHomeAutomationDeviceEndpoint
         * @memberof datastore
         * @function
         * @public
         * @param had {Object} The HA device
         * @param endpoint {String} The HA device endpoint ID
         * @param callback {Function} A callback to fire when finished
         */
        findGroundTruthForHomeAutomationDeviceEndpoint: function(had, endpoint, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findGroundTruthForHomeAutomationDeviceEndpoint',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "GROUND_TRUTH_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_EXTERNAL_ID" = $2 AND ' +
                            '  "ENDPOINT" = $3',
                        values: [
                            had.hpc_id,
                            had.external_id,
                            endpoint
                        ]
                    }, function(err, result) {
                        done();

                        var truth = result && result.rows ? result.rows[0] : null;
                        callback(err, truth);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all ground truth device entries for the given HA device
         *
         * @name deleteGroundTruthForHomeAutomationDevice
         * @memberof datastore
         * @function
         * @public
         * @param had {Object} The HA device
         * @param callback {Function} A callback to fire when finished
         */
        deleteGroundTruthForHomeAutomationDevice: function(had, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteGroundTruthForHomeAutomationDevice',
                        text:
                            'DELETE FROM ' +
                            '  "GROUND_TRUTH_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_EXTERNAL_ID" = $2 ',
                        values: [
                            had.hpc_id,
                            had.external_id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete all ground truth device entries for the given HA device
         *
         * @name deleteGroundTruthForHomeAutomationDeviceEndpoint
         * @memberof datastore
         * @function
         * @public
         * @param had {Object} The HA device
         * @param endpoint_id {String} The external identifier of the HA device endpoint
         * @param callback {Function} A callback to fire when finished
         */
        deleteGroundTruthForHomeAutomationDeviceEndpoint: function(had, endpoint, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteGroundTruthForHomeAutomationDeviceEndpoint',
                        text:
                            'DELETE FROM ' +
                            '  "GROUND_TRUTH_DEVICE" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 AND ' +
                            '  "HOME_AUTOMATION_DEVICE_EXTERNAL_ID" = $2 AND ' +
                            '  "ENDPOINT" = $3',
                        values: [
                            had.hpc_id,
                            had.external_id,
                            endpoint
                        ]
                    }, function(err, result) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Store the energy report tip record in the relational database
         *
         * @name addEnergyReportTip
         * @memberof datastore
         * @function
         * @public
         * @param tip {object} The energy report tip tag record to save
         * @param callback {Function} A callback with the usual error and result parameters
         */
        addEnergyReportTip: function(tip, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'addEnergyReporTip',
                        text:
                            'INSERT INTO "ENERGY_REPORT_TIP"( ' +
                            '  "USER_ID", ' +
                            '  "TAG", ' +
                            '  "REPORT_START", ' +
                            '  "REPORT_END", ' +
                            '  "REPORT_TYPE" ' +
                            ') VALUES ( ' +
                            '  $1, $2, $3, $4, $5 ' +
                            ') ' +
                            'RETURNING ' +
                            '  "ID" ',
                        values: [
                            tip.user_id,
                            tip.tag,
                            tip.report_start,
                            tip.report_end,
                            tip.report_type
                        ]
                    }, function(err, result) {
                        if (!err) {
                            tip.id = result.rows[0].ID;
                        }

                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find energy report tips for the given user in the period
         *
         * The result is ordered by the report_end field descendingly.
         *
         * @name findEnergyReportTipsForUser
         * @memberof datastore
         * @function
         * @public
         * @param username {string} The user's username
         * @param period {Object} The period, with both nullable fields:
         * @param period.start {Date} The start of the period
         * @param period.end {Date} The end of the period
         * @param callback {Function} A callback to fire when finished
         */
        findEnergyReportTipsForUser: function(username, period, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findEnergyReportTipsForUser',
                        text:
                            'SELECT ' +
                            '  "ENERGY_REPORT_TIP".* ' +
                            'FROM ' +
                            '  "ENERGY_REPORT_TIP" ' +
                            '  INNER JOIN "USER" ON "USER"."ID" = "ENERGY_REPORT_TIP"."USER_ID" ' +
                            'WHERE ' +
                            '  "USER"."USERNAME" = $1 AND ' +
                            // we specifically want an inclusive range end in this case
                            '  "ENERGY_REPORT_TIP"."REPORT_END" <@ tstzrange($2, $3, \'(]\') ' +
                            'ORDER BY ' +
                            '  "ENERGY_REPORT_TIP"."REPORT_END" DESC ',
                        values: [
                            username,
                            period.start || '-infinity',
                            period.end || 'infinity'
                        ]
                    }, function(err, result) {
                        done();

                        var tips = (result || {}).rows || [];
                        callback(err, tips);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find an organisation by its private identifier
         *
         * @name findOrganisationById
         * @memberof datastore
         * @function
         * @public
         * @param id {String} The private identifier of the organisation
         * @param callback {Function} A callback to fire when finished
         */
        findOrganisationById: function(id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findOrganisationById',
                        text:
                            'SELECT * FROM ' +
                            '  "ORGANISATION" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            id
                        ]
                    }, function(err, result) {
                        done();

                        var organisation = result && result.rows ? result.rows[0] : null;
                        callback(err, organisation);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find an organisation by its name.
         *
         * @name findOrganisationByName
         * @memberof datastore
         * @function
         * @public
         * @param name {String} The name of the organisation
         * @param callback {Function} A callback to fire when finished
         */
        findOrganisationByName: function(name, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findOrganisationByName',
                        text:
                            'SELECT * FROM ' +
                            '  "ORGANISATION" ' +
                            'WHERE ' +
                            '  "NAME" = $1 ',
                        values: [
                            name
                        ]
                    }, function(err, result) {
                        done();

                        var organisation = result && result.rows ? result.rows[0] : null;

                        callback(err, organisation);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find the set of all organisations, sorted by name
         *
         * @name findAllOrganisations
         * @memberof datastore
         * @function
         * @public
         * @param id {String} The private identifier of the organisation
         * @param callback {Function} A callback to fire when finished
         */
        findAllOrganisations: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findAllOrganisations',
                        text:
                            'SELECT * FROM ' +
                            '  "ORGANISATION" ' +
                            'ORDER BY "NAME" ASC'
                    }, function(err, result) {
                        done();

                        var organisations = (result || {}).rows || [];
                        callback(err, organisations);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find the set of all user roles
         *
         * @name findAllUserRoles
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback to fire when finished
         */
        findAllUserRoles: function(callback) {
            var roles = Object.keys(USER_ROLE).map(function(name) {
                return {
                    ID: USER_ROLE[name],
                    NAME: name
                };
            });
            callback(null, roles);
        },

        /**
         * Create or edit a permission entry
         *
         * @name addOrUpdatePermission
         * @memberof datastore
         * @function
         * @public
         * @param permission {Object} The permission object to create or update
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdatePermission: function(permission, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (permission.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updatePermission',
                            text:
                                'UPDATE ' +
                                '  "PERMISSION" ' +
                                'SET ' +
                                '  "ORGANISATION_ID" = $1 '+
                                'WHERE ' +
                                '  "ID" = $2 ',
                            values: [
                                permission.organisation.id,
                                permission.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addPermission',
                            text:
                                'INSERT INTO "PERMISSION"( ' +
                                '  "ORGANISATION_ID" ' +
                                ') VALUES ( ' +
                                '  $1 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                permission.organisation.id
                            ]
                        }, function(err, result) {
                            if (!err) {
                                permission.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a permission
         *
         * @name deletePermission
         * @memberof datastore
         * @function
         * @public
         * @param permission {Object} The permission to delete
         * @param callback Callback to fire when finished
         */
        deletePermission: function(permission, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deletePermission',
                        text:
                            'DELETE FROM ' +
                            '  "PERMISSION" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            permission.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find the details of the set of supplied tip identifiers and their
         * history of being sent to the supplied user
         *
         * @name findExternalTipSourceHistory
         * @memberof datastore
         * @function
         * @public
         * @param tips {Array} Array of external tip identifiers
         * @param user_id {Number} The private identifier of the user
         * @param callback {Function} A callback to fire when finished
         */
        findExternalTipSourceHistory: function(tips, user_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findExternalTipSourceHistory',
                        text:
                            'SELECT tip1.*, journ."TAG_SENT" as "TAG_SENT", COALESCE(hist1."SENT", \'1970-01-01\') as "SENT" FROM ' +
                            '"EXTERNAL_TIP_SOURCE" tip1 LEFT OUTER JOIN "APPLIANCE_TIP_HISTORY" hist1 ON ' +
                            '    tip1."ID" = hist1."TIP_ID" AND ' +
                            '    hist1."USER_ID" = $1 ' +
                            'INNER JOIN (' +
                            '    SELECT tip2."TAG", MAX(COALESCE(hist2."SENT", \'1970-01-01\')) AS "TAG_SENT" FROM ' +
                            '    "EXTERNAL_TIP_SOURCE" tip2 LEFT OUTER JOIN "APPLIANCE_TIP_HISTORY" hist2 ON ' +
                            '        tip2."ID" = hist2."TIP_ID" AND ' +
                            '        hist2."USER_ID" = $1 ' +
                            '    GROUP BY tip2."TAG" ' +
                            ') journ ON journ."TAG" = tip1."TAG" ' +
                            'WHERE ' +
                            '    tip1."EXTERNAL_ID" = ANY ($2) ' +
                            'ORDER BY "TAG_SENT", "SENT"',
                        values: [
                            user_id,
                            tips
                        ]
                    }, function(err, result) {
                        done();

                        var tips = (result || {}).rows || [];
                        callback(err, tips);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Record that this tip has been sent to users owning the supplied HPC
         *
         * @name saveExternalTipSourceSentToHPC
         * @memberof datastore
         * @function
         * @public
         * @param tip {Object} External tip source object
         * @param user_id {Number} The private identifier of the user that has had the tip
         * @param callback {Function} A callback to fire when finished
         */
        saveExternalTipSourceSentToHPC: function(tip, user_id, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    async.series([
                        function(callback2) {
                            clientQuery(client, true, {
                                name: 'insertExternalTipSourceSentToHPC',
                                text:
                                    'INSERT INTO "APPLIANCE_TIP_HISTORY" ' +
                                    ' ("USER_ID", "TIP_ID", "SENT") ' +
                                    'SELECT $1, $2, NOW() ' +
                                    'WHERE NOT EXISTS (' +
                                    '  SELECT * FROM "APPLIANCE_TIP_HISTORY" ' +
                                    '  WHERE ' +
                                    '    "USER_ID" = $1 AND ' +
                                    '    "TIP_ID" = $2 ' +
                                    ')',
                                values: [
                                    user_id,
                                    tip.id
                                ]
                            }, callback2);
                        }, function(callback2) {
                            clientQuery(client, true, {
                                name: 'updateExternalTipSourceSentToHPC',
                                text:
                                    'UPDATE "APPLIANCE_TIP_HISTORY" ' +
                                    'SET ' +
                                    '  "SENT" = NOW() '+
                                    'WHERE ' +
                                    '  "USER_ID" = $1 AND ' +
                                    '  "TIP_ID" = $2 ',
                                values: [
                                    user_id,
                                    tip.id
                                ]
                            }, callback2);
                        }
                    ], function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Create or edit a coupon code entry
         *
         * @name addOrUpdateCouponCode
         * @memberof datastore
         * @function
         * @public
         * @param coupon_code {Object} The coupon code object to create or update
         * @param callback {Function} A callback to fire when finished
         */
        addOrUpdateCouponCode: function(coupon_code, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (coupon_code.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateCouponCode',
                            text:
                                'UPDATE ' +
                                '  "COUPON_CODE" ' +
                                'SET ' +
                                '  "ACTIVE" = $1 '+
                                'WHERE ' +
                                '  "ID" = $2 ',
                            values: [
                                coupon_code.active,
                                coupon_code.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addCouponCode',
                            text:
                                'INSERT INTO "COUPON_CODE"( ' +
                                '  "CODE", ' +
                                '  "ORGANISATION_ID", ' +
                                '  "ACTIVE" ' +
                                ') VALUES ( ' +
                                '  $1, ' +
                                '  $2, ' +
                                '  $3 ' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                coupon_code.code,
                                (coupon_code.organisation || {}).id,
                                coupon_code.active
                            ]
                        }, function(err, result) {
                            if (!err) {
                                coupon_code.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },


        /**
         * Find a coupon code by its public code string
         *
         * @name findCouponCodeByCode
         * @memberof datastore
         * @function
         * @public
         * @param code {String} The public code string of the coupon code
         * @param callback {Function} A callback to fire when finished
         */
        findCouponCodeByCode: function(code, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findCouponCodeByCode',
                        text:
                            'SELECT * FROM ' +
                            '  "V_COUPON_CODE" ' +
                            'WHERE ' +
                            '  lower("CODE") = lower($1) ',
                        values: [
                            code
                        ]
                    }, function(err, result) {
                        done();

                        var coupon_code = result && result.rows ? result.rows[0] : null;
                        callback(err, coupon_code);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find a coupon code by its public code string
         *
         * @name findCouponCodesForUser
         * @memberof datastore
         * @function
         * @public
         * @param organisation_id {Number} Optional private identifier for the
         * organisation that owns the coupon codes. If none is supplied, all coupon
         * codes will be returned.
         * @param callback {Function} A callback to fire when finished
         */
        findCouponCodesForOrganisation: function(organisation_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    var sql =
                        'SELECT * FROM ' +
                        '  "V_COUPON_CODE" ';
                    var params = [];

                    if (organisation_id) {
                        sql += 'WHERE "ORGANISATION_ID" = $1 ';
                        params.push(organisation_id);
                    }

                    sql += 'ORDER BY "CREATED" DESC';

                    clientQuery(client, false, {
                        name: 'findCouponCodesForOrganisation' + (organisation_id ? '_withOrganisation' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        var coupon_codes = result ? result.rows : null;
                        done();

                        callback(err, coupon_codes);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a solar efficiency result
         *
         * @name deleteSolarEfficiencyResult
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_efficiency_result {Object}
         *   The solar efficiency result to delete
         * @param callback {Function}
         *   A callback to fire when finished with the usual error parameter
         */
        deleteSolarEfficiencyResult: function(solar_efficiency_result, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteSolarEfficiencyResult',
                        text:
                            'DELETE FROM ' +
                            '  "SOLAR_EFFICIENCY_RESULT" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            solar_efficiency_result.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Create or edit a solar efficiency result entry. Note that unlike addOrUpdate... functions,
         * the entry does not have to have been read before an update. Instead, there will be only one
         * row per algorthim result per HPC per day.
         *
         * @name storeSolarEfficiencyHistory
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_efficiency_history {Object}
         *   The solar efficiency result to store.
         * @param callback {Function}
         *   A callback to fire when finished with the usual error parameter
         */
        storeSolarEfficiencyHistory: function(solar_efficiency_history, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    // Note that the following code is NOT a true upsert. It will fail concurrent writes and
                    // deletes but it should be sufficient for our needs in this case.
                    clientQuery(client, true, {
                        name: 'insertSolarEfficiencyHistory',
                        text:
                            'INSERT INTO  "SOLAR_EFFICIENCY_HISTORY" (' +
                            '  "HPC_ID", ' +
                            '  "CALCULATED_FOR_DATE", ' +
                            '  "ALGORITHM_DESCRIPTION", ' +
                            '  "RESULT_VALUE", ' +
                            '  "EFFICIENT" ' +
                            ') SELECT ' +
                            '    $1, ' +
                            '    $2, ' +
                            '    $3::VARCHAR, ' + // Postgres needs a hint to distinguish between text and varchar here
                            '    $4, ' +
                            '    $5' +
                            '  WHERE NOT EXISTS (' +
                            '    SELECT * FROM "SOLAR_EFFICIENCY_HISTORY" ' +
                            '    WHERE ' +
                            '       "HPC_ID" = $1 AND ' +
                            '       "CALCULATED_FOR_DATE" = $2 AND ' +
                            '       "ALGORITHM_DESCRIPTION" = $3 ' +
                            '  ) ' +
                            'RETURNING ' +
                            '  "ID" ',
                        values: [
                            solar_efficiency_history.hpc_id,
                            solar_efficiency_history.calculated_for_date,
                            solar_efficiency_history.algorithm_description,
                            solar_efficiency_history.result_value,
                            solar_efficiency_history.efficient
                        ]
                    }, function(err, result) {
                        if (!err && result.rowCount === 0) {
                            clientQuery(client, true, {
                                name: 'updateSolarEfficiencyHistory',
                                text:
                                    'UPDATE ' +
                                    '  "SOLAR_EFFICIENCY_HISTORY" ' +
                                    'SET ' +
                                    // HPC_ID, CALCULATED_FOR_DATE and ALGORITHM_DESCRIPTION columns never update once
                                    // created.
                                    '  "RESULT_VALUE" = $4, ' +
                                    '  "EFFICIENT" = $5 ' +
                                    'WHERE ' +
                                    '  "HPC_ID" = $1 AND ' +
                                    '  "CALCULATED_FOR_DATE" = $2 AND ' +
                                    '  "ALGORITHM_DESCRIPTION" = $3',
                                values: [
                                    solar_efficiency_history.hpc_id,
                                    solar_efficiency_history.calculated_for_date,
                                    solar_efficiency_history.algorithm_description,
                                    solar_efficiency_history.result_value,
                                    solar_efficiency_history.efficient
                                ]
                            }, function(err) {
                                done();
                                callback(err);
                            });
                        } else {
                            // There's no model object so we do not store the generated ID.
                            // The id is not used to determine whether to insert or update.

                            done();
                            callback(err);
                        }
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Retrieve the most recent solar efficiency results for a given HPC,
         * job type (e.g. max_voltage_1day) and number of days.
         *
         * @name getSolarEfficiencyHistory
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The internal ID of the HPC
         * @param days {Number} The number of days to retrieve efficiency status for
         * @param job_name {String} The name of the efficiency job (e.g. 'max_voltage_1day')
         * @param callback {Function} Callback function taking as parameters:
         *                      err {Error} Any unhandled error
         *                      results {Array} Array of solar efficiency results ordered by date (desc)
         */
        getSolarEfficiencyHistory: function(hpc_id, days, job_type, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'getSolarEfficiencyHistory',
                        text:
                        'SELECT ' +
                        '  * ' +
                        'FROM ' +
                        '  "SOLAR_EFFICIENCY_HISTORY" ' +
                        'WHERE ' +
                        '  "HPC_ID" = $1 ' +
                        '  AND ' +
                        '  "ALGORITHM_DESCRIPTION" = $2 ' +
                        'ORDER BY ' +
                        '  "CALCULATED_FOR_DATE" DESC ' +
                        'LIMIT $3',
                        values: [
                            hpc_id,
                            job_type,
                            days
                        ]
                    }, function(err, result) {
                        done();
                        callback(err, result.rows);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete an HPC's solar efficiency result history
         *
         * @name deleteSolarEfficiencyHistoryForHpc
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC that will have its solar efficiency history deleted
         * @param callback {Function}
         *   A callback to fire when finished with the usual error parameter
         */
        deleteSolarEfficiencyHistoryForHpc: function(hpc_id, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteSolarEfficiencyHistoryForHpc',
                        text:
                            'DELETE FROM ' +
                            '  "SOLAR_EFFICIENCY_HISTORY" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ',
                        values: [
                            hpc_id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Create or edit a solar efficiency result entry
         *
         * @name addOrUpdateSolarEfficiencyResult
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_efficiency_result {Object}
         *   The solar efficiency result to create or edit. When inserting, this
         *   object is modified inside the function.
         * @param callback {Function}
         *   A callback to fire when finished with the usual error parameter
         */
        addOrUpdateSolarEfficiencyResult: function(solar_efficiency_result, callback) {
            pg.connect(connection, function(err, client, done) {
                if (!err) {
                    // Are we updating or inserting?
                    if (solar_efficiency_result.id) {
                        // It's an update
                        clientQuery(client, true, {
                            name: 'updateSolarEfficiencyResult',
                            text:
                                'UPDATE ' +
                                '  "SOLAR_EFFICIENCY_RESULT" ' +
                                'SET ' +
                                // HPC_ID and CREATED columns never updates once
                                // created.
                                '  "RESULTS" = $1, ' +
                                '  "UPDATED" = $2, ' +
                                '  "LAST_CONNECTED" = $3 ' +
                                'WHERE ' +
                                '  "ID" = $4 ',
                            values: [
                                solar_efficiency_result.results,
                                solar_efficiency_result.updated,
                                solar_efficiency_result.last_connected,
                                solar_efficiency_result.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        // It's an insert
                        clientQuery(client, true, {
                            name: 'addSolarEfficiencyResult',
                            text:
                                'INSERT INTO "SOLAR_EFFICIENCY_RESULT"( ' +
                                '  "HPC_ID", ' +
                                '  "RESULTS", ' +
                                '  "LAST_CONNECTED" ' +
                                ') VALUES ( ' +
                                '  $1, ' +
                                '  $2, ' +
                                '  $3' +
                                ') ' +
                                'RETURNING ' +
                                '  "ID" ',
                            values: [
                                solar_efficiency_result.hpc_id,
                                solar_efficiency_result.results,
                                solar_efficiency_result.last_connected
                            ]
                        }, function(err, result) {
                            if (!err) {
                                solar_efficiency_result.id = result.rows[0].ID;
                            }

                            done();
                            callback(err);
                        });
                    }
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find the current solar efficiency result for the supplied HPC
         *
         * @name findSolarEfficiencyResultForHPC
         * @memberof datastore.postgres
         * @function
         * @public
         * @param hpc_id {Number} The private identifier of the HPC
         * @param callback {Function} A callback with the parameters:
         *   error - The error
         *   result - a DTO of SolarEfficiencyResult model
         */
        findSolarEfficiencyResultForHPC: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSolarEfficiencyResultForHPC',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "SOLAR_EFFICIENCY_RESULT" ' +
                            'WHERE ' +
                            ' "HPC_ID" = $1 ' +
                            // No uniqueness on hpc_id in the schema, latest wins
                            'ORDER BY "ID" DESC ' +
                            'LIMIT 1',
                        values: [
                            hpc_id
                        ]
                    }, function(err, result) {
                        var solar_efficiency_result = result && result.rows ? result.rows[0] : null;
                        done();
                        callback(err, solar_efficiency_result);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search for recurring job records that are marked as active in the RECURRING_JOB table excepting
         *  the active job that has job_status_report as its job handler function
         *
         * @name findActiveRecurringJobs
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} A callback with the usual error and result parameters
         */
        findActiveRecurringJobs: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findActiveRecurringJobs',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "RECURRING_JOB" ' +
                            'WHERE ' +
                            ' "ACTIVE" = true AND ' +
                            ' "JOB_HANDLER" != $1 ',
                        values: ['job_status_report']
                    }, function(err, result) {
                        var jobs = result && result.rows || [];
                        done();
                        callback(err, jobs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find solar_detail record for a HPC.
         *
         * @name findSolarDetailsForHpc
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {Number} Internal HPC ID
         * @param callback {Function} Taking parameters:
         *                      err {Error} if any
         *                      row {Object} if found, otherwise null
         */
        findSolarDetailsForHpc: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSolarDetailsForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "SOLAR_DETAIL" ' +
                            'WHERE ' +
                            ' "HPC_ID" = $1 ',
                        values: [hpc_id]
                    }, function(err, result) {
                        var row = result && result.rows ? result.rows[0] : null;
                        done();
                        callback(err, row);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find solar_baseline record for a HPC.
         *
         * @name findSolarBaselineForHpc
         * @memberof datastore
         * @function
         * @public
         * @param hpc_id {Number} Internal HPC ID
         * @param callback {Function} Taking parameters:
         *                      err {Error} if any
         *                      row {Object} if found, otherwise null
         */
        findSolarBaselineForHpc: function(hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findSolarBaselineForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "SOLAR_BASELINE" ' +
                            'WHERE ' +
                            ' "HPC_ID" = $1 ',
                        values: [hpc_id]
                    }, function(err, result) {
                        var row = result && result.rows ? result.rows[0] : null;
                        done();
                        callback(err, row);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find solar metadata and baseline information for all HPCs with an associated
         * SOLAR_DETAIL record
         *
         * @name findHpcsWithSolar
         * @memberof datastore
         * @function
         * @public
         * @param callback {Function} Callback taking as parameters:
         *                                err {Error} Optional error, if any
         *                                results {Array} Objects representing a V_SOLAR record
         *                                                with joined HPC serial
         */
        findHpcsWithSolar: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'findHpcsWithSolar',
                        text:
                            'SELECT ' +
                            '  "V_SOLAR".*, ' +
                            '  "HPC"."SERIAL" ' +
                            'FROM ' +
                            '  "V_SOLAR" ' +
                            '  JOIN "HPC" ' +
                            '    ON "V_SOLAR"."SOLAR_DETAIL_HPC_ID"="HPC"."ID" ',
                        values: []
                    }, function(err, result) {
                        done();
                        var solar_installs = result && result.rows ? result.rows : [];
                        callback(err, solar_installs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Save SolarBaseline values to the database, inserting a new entry if necessary.
         *
         * @name addOrUpdateSolarBaseline
         * @memberof datastore
         * @function
         * @public
         * @param solar_baseline {Object} SolarBaseline model instance (modified in-place with new ID if inserted)
         * @param callback {Function} Callback with optional error parameter
         */
        addOrUpdateSolarBaseline: function(solar_baseline, callback) {
            if (solar_baseline.id) {
                // Update
                pgConnect(connection, function(err, client, done) {
                    if (!err) {
                        clientQuery(client, true, {
                            name: 'updateSolarBaseline',
                            text:
                                'UPDATE ' +
                                '  "SOLAR_BASELINE" ' +
                                'SET ' +
                                // Note: HPC_ID is never changed by an update
                                '  "DAILY_GENERATION_MAX_DATE" = $1, ' +
                                '  "DAILY_GENERATION_MAX" = $2, ' +
                                '  "MAX_POWER" = $3 ' +
                                'WHERE ' +
                                '  "ID" = $4 ',
                            values: [
                                solar_baseline.daily_generation_max_date,
                                solar_baseline.daily_generation_max,
                                solar_baseline.max_power,
                                solar_baseline.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        done();
                        callback(err);
                    }
                });
            } else {
                // Insert
                pgConnect(connection, function(err, client, done) {
                    if (!err) {
                        clientQuery(client, true, {
                            name: 'insertSolarBaseline',
                            text:
                                'INSERT INTO ' +
                                '  "SOLAR_BASELINE" (' +
                                '    "HPC_ID", ' +
                                '    "MAX_POWER", ' +
                                '    "DAILY_GENERATION_MAX", ' +
                                '    "DAILY_GENERATION_MAX_DATE" ' +
                                '  ) VALUES ( ' +
                                '    $1, $2, $3, $4' +
                                '  ) ' +
                                '  RETURNING ' +
                                '    "ID" ',
                            values: [
                                solar_baseline.hpc_id,
                                solar_baseline.max_power,
                                solar_baseline.daily_generation_max,
                                solar_baseline.daily_generation_max_date
                            ]
                        }, function(err, result) {
                            if (!err) {
                                solar_baseline.id = result.rows[0].ID;
                            }
                            done();
                            callback(err);
                        });
                    } else {
                        done();
                        callback(err);
                    }
                });
            }
        },

        /**
         * Delete a solar baseline record
         *
         * @name deleteSolarBaseline
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_baseline {Object} The solar baseline record to delete
         * @param callback {Function} A callback to run when finished, with any error
         */
        deleteSolarBaseline: function(solar_baseline, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteSolarBaseline',
                        text:
                            'DELETE FROM ' +
                            '  "SOLAR_BASELINE" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            solar_baseline.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Save SolarDetails values to the database, inserting a new entry if necessary.
         *
         * Note: the update intentionally skips the MONITORING and ALERT_STATUS columns, which are handled
         * separately in saveSolarMonitoringConfiguration and saveSolarAlertStatus to simplify access control
         * and avoid races between data coming from different sources.
         *
         * @name addOrUpdateSolarDetails
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_details {Object} SolarDetails model instance (modified in-place with new ID if inserted)
         * @param callback {Function} Callback with optional error parameter
         */
        addOrUpdateSolarDetails: function(solar_details, callback) {
            if (solar_details.id) {
                // Update
                pgConnect(connection, function(err, client, done) {
                    if (!err) {
                        clientQuery(client, true, {
                            name: 'updateSolarDetails',
                            text:
                                'UPDATE ' +
                                '  "SOLAR_DETAIL" ' +
                                'SET ' +
                                // Note: HPC_ID is never changed by an update
                                '  "CAPACITY" = $1, ' +
                                '  "DATA" = $2 ' +
                                'WHERE ' +
                                '  "ID" = $3 ',
                            values: [
                                solar_details.capacity,
                                solar_details.data,
                                solar_details.id
                            ]
                        }, function(err) {
                            done();
                            callback(err);
                        });
                    } else {
                        done();
                        callback(err);
                    }
                });
            } else {
                // Insert
                pgConnect(connection, function(err, client, done) {
                    if (!err) {
                        clientQuery(client, true, {
                            name: 'insertSolarDetails',
                            text:
                                'INSERT INTO ' +
                                '  "SOLAR_DETAIL" (' +
                                '    "HPC_ID", ' +
                                '    "CAPACITY", ' +
                                '    "DATA" ' +
                                '  ) VALUES ( ' +
                                '    $1, $2, $3' +
                                '  ) ' +
                                '  RETURNING ' +
                                '    "ID" ',
                            values: [
                                solar_details.hpc_id,
                                solar_details.capacity,
                                solar_details.data,
                            ]
                        }, function(err, result) {
                            if (!err) {
                                solar_details.id = result.rows[0].ID;
                            }
                            done();
                            callback(err);
                        });
                    } else {
                        done();
                        callback(err);
                    }
                });
            }
        },

        /**
         * Save monitoring configuration for an existing solar system
         *
         * @name saveSolarMonitoringConfiguration
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_details {Object} SolarDetails model instance
         * @param callback {Function} Callback with optional error parameter
         */
        saveSolarMonitoringConfiguration: function(solar_details, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'updateSolarMonitoringConfiguration',
                        text:
                            'UPDATE ' +
                            '  "SOLAR_DETAIL" ' +
                            'SET ' +
                            '  "MONITORING" = $1 ' +
                            'WHERE' +
                            '  "ID" = $2 ',
                        values: [
                            solar_details.monitoring,
                            solar_details.id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Save alert status for an existing solar system
         *
         * This is intentionally separate from addOrUpdateSolarDetails to avoid races between
         * data updated via form vs data updated via cron job.
         *
         * @name saveSolarAlertStatus
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_details {Object} SolarDetails model instance
         * @param callback {Function} Callback with optional error parameter
         */
        saveSolarAlertStatus: function(solar_details, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'updateSolarAlertStatus',
                        text:
                            'UPDATE ' +
                            '  "SOLAR_DETAIL" ' +
                            'SET ' +
                            '  "ALERT_STATUS" = $1 ' +
                            'WHERE' +
                            '  "ID" = $2 ',
                        values: [
                            solar_details.alert_status,
                            solar_details.id
                        ]
                    }, function(err, result) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a solar details record
         *
         * @name deleteSolarDetails
         * @memberof datastore.postgres
         * @function
         * @public
         * @param solar_details {Object} The solar details record to delete
         * @param callback {Function} A callback to run when finished, with any error
         */
        deleteSolarDetails: function(solar_details, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteSolarDetails',
                        text:
                            'DELETE FROM ' +
                            '  "SOLAR_DETAIL" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            solar_details.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Add a new scheduled notification to the database
         *
         * @name addScheduledNotification
         * @memberof datastore.postgres
         * @function
         * @public
         * @param scheduled_notification {Object} The ScheduledNotification model instance to insert
         *                                        Note: modified in-place with resulting row ID
         * @param callback {Function} A callback to run when finished, with any error
         */
        addScheduledNotification: function(scheduled_notification, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'addScheduledNotification',
                        text:
                            'INSERT INTO ' +
                            '  "SCHEDULED_NOTIFICATION" (' +
                            '    "TIMESTAMP", ' +
                            '    "HPC_ID", ' +
                            '    "HANDLER", ' +
                            '    "DATA" ' +
                            '  ) VALUES ( ' +
                            '    $1, $2, $3, $4' +
                            '  ) ' +
                            '  RETURNING ' +
                            '    "ID" ',
                        values: [
                            scheduled_notification.timestamp,
                            scheduled_notification.hpc_id,
                            scheduled_notification.handler,
                            scheduled_notification.data,
                        ]
                    }, function(err, result) {
                        if (!err) {
                            scheduled_notification.id = result.rows[0].ID;
                        }
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Find all scheduled notifications that are ready to be sent
         *
         * @name getScheduledNotifications
         * @memberof datastore.postgres
         * @function
         * @public
         * @param callback {Function} A callback to run when finished with:
         *                     err     {Error} Any error which occurred during query
         *                     results {Array} List of scheduled notification records
         */
        getScheduledNotifications: function(callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'getScheduledNotifications',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "SCHEDULED_NOTIFICATION" ' +
                            'WHERE ' +
                            '  "TIMESTAMP" < NOW() ',
                        values: []
                    }, function(err, result) {
                        done();
                        var notifications = result && result.rows ? result.rows : [];
                        callback(err, notifications);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Get all scheduled notifications of a given type for a given HPC
         *
         * @name getScheduledNotificationTypeForHpc
         * @memberof datastore.postgres
         * @function
         * @public
         * @param handler  {String} Notification type
         * @param hpc_id   {Number} Internal HPC ID
         * @param callback {Function} A callback to run when finished, with any error
         */
        getScheduledNotificationTypeForHpc: function(handler, hpc_id, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    clientQuery(client, false, {
                        name: 'getScheduledNotificationTypeForHpc',
                        text:
                            'SELECT ' +
                            '  * ' +
                            'FROM ' +
                            '  "SCHEDULED_NOTIFICATION" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            'AND ' +
                            '  "HANDLER" = $2',
                        values: [
                            hpc_id,
                            handler
                        ]
                    }, function(err, result) {
                        done();
                        var notifications = result && result.rows ? result.rows : [];
                        callback(err, notifications);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a scheduled notification from the database
         *
         * @name deleteScheduledNotification
         * @memberof datastore.postgres
         * @function
         * @public
         * @param notification {Object} ScheduledNotification instance to delete
         * @param callback {Function} A callback to run when finished, with any error
         */
        deleteScheduledNotification: function(notification, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteScheduledNotification',
                        text:
                            'DELETE FROM ' +
                            '  "SCHEDULED_NOTIFICATION" ' +
                            'WHERE ' +
                            '  "ID" = $1 ',
                        values: [
                            notification.id
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Delete a given type of scheduled notification for a given HPC
         *
         * @name deleteNotificationTypeForHpc
         * @memberof datastore.postgres
         * @function
         * @public
         * @param handler  {String} Notification type to delete
         * @param hpc_id   {Number} Internal HPC ID
         * @param callback {Function} A callback to run when finished, with any error
         */
        deleteNotificationTypeForHpc: function(handler, hpc_id, callback) {
            pgConnect(connectionDelete, function(err, client, done) {
                if (!err) {
                    clientQuery(client, true, {
                        name: 'deleteNotificationTypeForHpc',
                        text:
                            'DELETE FROM ' +
                            '  "SCHEDULED_NOTIFICATION" ' +
                            'WHERE ' +
                            '  "HPC_ID" = $1 ' +
                            'AND ' +
                            '  "HANDLER" = $2 ',
                        values: [
                            hpc_id,
                            handler
                        ]
                    }, function(err) {
                        done();
                        callback(err);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        },

        /**
         * Search against a full-text index
         *
         * @name search
         * @memberof datastore
         * @function
         * @public
         * @param search {String} The user-supplied string to search
         * @param organisation {Number} Optional organisation identifier to which results get limited
         * @param limit {Number} The maximum number of results to retrieve
         * @param offset {Number} The offset of the results to retrieve
         * @param callback {Function} A callback with the usual error and result parameters
         */
        search: function(search, organisation, limit, offset, callback) {
            pgConnect(connection, function(err, client, done) {
                if (!err) {
                    // to_tsquery must have explicit operators separating
                    // the tokens when multiple tokens are present.
                    //
                    // Note that it is possible for the user to generate an
                    // error here, if they use incorrect query syntax, e.g.
                    // 'foo && bar'. That error is intercepted below, so that
                    // it can be reported back to the user in a nicer way.
                    // Ideally, we would validate the query syntax at runtime
                    // instead, but that would require implementing the
                    // to_tsquery parsing logic in JavaScript and keeping that
                    // in sync with PostgreSQL, so that is not really feasible.
                    //
                    // An alternative would be to use plainto_tsquery, but
                    // then Boolean operators could not be used.
                    var querytext = search.replace(/\s+/g, '&');

                    var sql =
                        'SELECT ' +
                        '  COUNT("ID") OVER () AS "TOTAL", ' +
                        '  "TYPE", ' +
                        '  "ID", ' +
                        '  "TITLE", ' +
                        '  "DESCRIPTION", ' +
                        '  "URL" ' +
                        'FROM ' +
                        '  ( SELECT * FROM "V_SEARCH" ) "FT_SEARCH" ' +
                        'WHERE ' +
                        '  "FT_SEARCH"."DOCUMENT" @@ to_tsquery($1) ';

                    if (organisation) {
                        sql += ' AND "ORGANISATION_ID" = $2 AND "TYPE" <> \'HPC\'';
                    }

                    sql +=
                        'ORDER BY ' +
                        '  ts_rank("FT_SEARCH"."DOCUMENT", to_tsquery($1)) DESC ';
                    var params = [ querytext ];

                    if (organisation) {
                        params.push(organisation);
                        if (limit) {
                            sql +=
                                'OFFSET ' +
                                '  $3 ' +
                                'LIMIT ' +
                                '  $4 ';
                            params.push(offset, limit);
                        }
                    } else {
                        if (limit) {
                            sql +=
                                'OFFSET ' +
                                '  $2 ' +
                                'LIMIT ' +
                                '  $3 ';
                            params.push(offset, limit);
                        }
                    }

                    clientQuery(client, false, {
                        name: 'search' + (organisation ? '_withOrganisation' : '') + (limit ? '_withLimit' : ''),
                        text: sql,
                        values: params
                    }, function(err, result) {
                        done();

                        var srs = result && result.rows ? result.rows : [];

                        // Intercept syntax error, and make it easier to deal
                        // with (recognise) by callers.
                        if (err && err.code === '42601') {
                            err = new Error('Query syntax error');
                        }

                        callback(err, srs);
                    });
                } else {
                    done();
                    callback(err);
                }
            });
        }
    };
}());
