/**
 * @file meter-connect/index.js
 * Functional tests for the meter-connect page
 */

(function() {
    "use strict";

    //Module dependencies
    var horseman = require('test/lib/browser/horseman.js');
    var mqtt     = require('mqtt');

    /*jshint -W071 */
    describe('Meter connect page', function(){
        this.timeout(Math.max(this.timeout(), 10000));

        var browser;
        var client;

        before(function() {
            browser = horseman.browserInit();
            return browser
                .do(function(done) {
                    client = mqtt.connect().on('connect', done);
                });
        });

        beforeEach(function() {
            var ct = this.currentTest;
            return browser.log(ct.parent.title + ' ' + ct.title);
        });

        describe('Australian user', function() {
            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/meter-connect', 'onboard12', 'onboard12')
                    .visible('#meter-connect-pane').should.finally.be.true();
            });

            it('should be available after login', function(){
                return browser
                    .select('#provider-selector', 'Other')
                    .screenshot('test/screenshots/meter_connect_1.png')
                    .expect([
                        { selector: '#meter-connect-pane', visible: true },
                        { selector: '#unknown',            visible: true }
                    ]);
            });

            it('should allow Citipower instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'Citipower')
                    .screenshot('test/screenshots/meter_connect_2.png')
                    .expect([
                        { selector: '#unknown',  visible: false },
                        { selector: '#powercor', visible: true }
                    ]);
            });

            it('should allow Powercor instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'Powercor')
                    .screenshot('test/screenshots/meter_connect_3.png')
                    .expect([
                        { selector: '#powercor', visible: true }
                    ]);
            });

            it('should allow Jemena instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'Jemena')
                    .screenshot('test/screenshots/meter_connect_4.png')
                    .expect([
                        { selector: '#powercor', visible: false },
                        { selector: '#jemena',   visible: true }
                    ]);
            });

            it('should allow United Energy instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'United Energy')
                    .screenshot('test/screenshots/meter_connect_5.png')
                    .expect([
                        { selector: '#jemena',       visible: false },
                        { selector: '#unitedenergy', visible: true }
                    ]);
            });

            it('should allow Ausnet Services instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'Ausnet Services')
                    .screenshot('test/screenshots/meter_connect_6.png')
                    .expect([
                        { selector: '#unitedenergy',   visible: false },
                        { selector: '#ausnetservices', visible: true }
                    ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe('United States user', function() {
            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/meter-connect', 'onboard13', 'onboard13')
                    .visible('#meter-connect-pane').should.finally.be.true();
            });

            it('should allow PG&E instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'PG&E')
                    .screenshot('test/screenshots/meter_connect_8.png')
                    .expect([
                        { selector: '#ausnetservices', visible: false },
                        { selector: '#pge',            visible: true }
                    ]);
            });

            it('should allow SCE instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'SCE')
                    .screenshot('test/screenshots/meter_connect_9.png')
                    .expect([
                        { selector: '#pge', visible: false },
                        { selector: '#sce', visible: true }
                    ]);
            });

            it('should allow SDG&E instructions to be selected', function(){
                return browser
                    .select('#provider-selector', 'SDG&E')
                    .screenshot('test/screenshots/meter_connect_10.png')
                    .expect([
                        { selector: '#sce',  visible: false },
                        { selector: '#sdge', visible: true }
                    ]);
            });

            it('should have registration progress properly set', function(){
                var pfx = '.user-profile .list-group-item:nth-child';
                return browser.expect([
                    { selector: '.user-profile',                  visible: true },
                    { selector: '.user-profile .user-profile-progress .list-group-item', count: 3 },
                    { selector: pfx + '(1) i.fa-circle.complete', visible: true },
                    { selector: pfx + '(2) i.fa-circle.complete', visible: true },
                    { selector: pfx + '(3) i.fa-circle.started',  visible: true },
                    { selector: pfx + '(1) .item-id',             exists: false },
                    { selector: pfx + '(2) .item-id',             exists: false },
                    { selector: pfx + '(3) .item-id',             text: '3' },
                    { selector: pfx + '(1) .progress-name',       text: 'Register' },
                    { selector: pfx + '(2) .progress-name',       text: 'Wi-Fi' },
                    { selector: pfx + '(3) .progress-name',       text: 'Meter' }
                ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe('Incomplete fuse box meter user', function() {
            before(function() {
                return browser
                .login('http://127.0.0.1:3000/meter-connect', 'onboard19', 'onboard19')
                .expect({ selector: '#meter-connect-pane', visible: true });
            });

            it('should show fuse meter page without Ha device', function() {
                return browser
                    .click('#meter_choose div:nth-of-type(2) input')
                    .screenshot('test/screenshots/meter_connect_12.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        { selector: '.table-tr',        exists: false },
                        { selector: '#fusebox-message-content', visible: false }
                    ]);
            });

            it('should show bad connection message in fuse meter page', function() {
                this.slow(5000);
                // This usually takes at least 4 seconds.
                return browser
                    .click('.btn-zigbeeha-pairing')
                    .waitForSelector('#fusebox-message:visible')
                    .screenshot('test/screenshots/meter_connect_13.png')
                    .html('#fusebox-message-content')
                    .should.finally.equal('There was a problem when trying to add a device. Check that your Emberpulse is connected to the Internet by checking your <a class="alert-link" href="/ihd">system status</a>. Then try again.');
            });

            it('should show searching message in fuse meter page', function() {
                return browser
                    .evaluate(function() { $('#fusebox-message').addClass('hidden'); })
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/pairing', '{"timeout": 180}', done);
                    })
                    .waitForSelector('#fusebox-message:visible')
                    .screenshot('test/screenshots/meter_connect_14.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        { selector: '.table-tr', exists: false },
                        { selector: '#fusebox-message-content', text: 'Searching for devices ...' }
                    ]);
            });

            it('should show no fuse meter message in fuse meter page', function() {
                return browser
                    .evaluate(function() { $('#fusebox-message').addClass('hidden'); })
                    .click('.btn-zigbeeha-pairing')
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/pairing', '{"timeout": 0}', done);
                    })
                    .waitForSelector('#fusebox-message:visible')
                    .screenshot('test/screenshots/meter_connect_15.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        { selector: '.table-tr', exists: false },
                        { selector: '#fusebox-message-content', text: 'No devices found. Please check the instructions below and try again.' }
                    ]);
            });

            var node = {
                name: 'Fuse Box Meter 1',
                parent_nwk: 'F216',
                manufacturer: 'Saturn South',
                model: 'Ignored-Model',
                rssi: -32,
                lqi: 85,
                discovery: 'in_progress',
                endpoints: {
                    count: 1,
                    items: [{
                        id: '0B',
                        discovery: 'in_progress',
                        name: 'Fuse Meter Box',
                        version: 0,
                        clusters: {
                            items: [
                                {
                                    id: '0300',
                                    discovery: 'in_progress',
                                    attributes: {
                                        items: [{
                                            id:      '00FF',
                                            type:    '42',
                                            updated: (new Date()).toISOString(),
                                            value:   'do not save'
                                        }]
                                    }
                                },
                                {
                                    // Set some SaturnSouth energy usage to make
                                    // the device appear as active.
                                    id: '0702',
                                    discovery: 'in_progress',
                                    attributes: {
                                        items: [{
                                            id:      'E140',
                                            type:    null,
                                            updated: (new Date()).toISOString(),
                                            value:   123000
                                        }]
                                    }
                                },
                            ]
                        }
                    }]
                }
            };

            it('show pairing message in fuse meter page', function() {
                return browser
                    .evaluate(function() { $('#fusebox-message').addClass('hidden'); })
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/nwk/F216/node/001BC502B0215F94', JSON.stringify(node), done);
                    })
                    // Allow the client some time to set up the handler before throwing it messages.
                    // In the past, these were racing and causing client-side code to only execute in some test runs.
                    .wait(1000)
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/nwk/F216/node/001BC502B0215F94/link_status', '{"rssi": -30, "lqi": 80}', done);
                    })
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/pairing', '{"timeout": 180}', done);
                    })
                    .waitForSelector('#fusebox-message:visible')
                    .screenshot('test/screenshots/meter_connect_16.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        { selector: '.table-tr', count: 1 },
                        { selector: '#fusebox-message-content', text: 'Finalising the connection. This may take some time. Please be patient ...' }
                    ]);
            });

            it('still show pairing message when timeout in fuse meter page', function() {
                // The message text doesn't change against the previous case.
                return browser
                    .evaluate(function() {
                        window.ipm.fusebox_message_content_state = 'inited';
                        var ha_list = window.ipm.widgets['widget-ha-devices-list'];
                        ha_list.socket.once(ha_list.options.subscribe.pairing, function() {
                            window.ipm.fusebox_message_content_state = 'changed';
                        });
                    })
                    .waitFor(function() { return window.ipm.fusebox_message_content_state; }, 'inited')
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/pairing', '{"timeout": 0}', done);
                    })
                    .waitFor(function() { return window.ipm.fusebox_message_content_state; }, 'changed')
                    .screenshot('test/screenshots/meter_connect_17.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        { selector: '.table-tr', count: 1 },
                        { selector: '#fusebox-message-content', text: 'Finalising the connection. This may take some time. Please be patient ...' }
                    ]);
            });

            it('show success message in fuse meter page', function() {
                return browser
                    .evaluate(function() { $('#fusebox-message').addClass('hidden'); })
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/pairing', '{"timeout": 0}', done);
                    })
                    .do(function(done) {
                        node.discovery = 'succeeded';
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/nwk/F216/node/001BC502B0215F94', JSON.stringify(node), done);
                    })
                    .do(function(done) {
                        client.publish('/frm_hpc/ha_meter_test_hpc/zigbeeha/nwk/F216/node/001BC502B0215F94/update', '', done);
                    })
                    .waitForSelector('#fusebox-message:visible')
                    .screenshot('test/screenshots/meter_connect_18.png')
                    .expect([
                        { selector: '#widget_fuse_box', visible: true },
                        // Fix Me: related to RAD-1812, remove the message matching temporarily
                        { selector: '.table-tr', count: 1 }
                        /*
                        { selector: '.table-tr', count: 1 },
                        { selector: '#fusebox-message-content', text: 'Success! Click on the newly added device below to configure.' }
                        */
                    ]);

            });

            it('show instruction message with meter setup form in home-automation page', function() {
                return browser
                    .click('tbody tr:eq(0)')
                    .waitForNextPage()
                    .screenshot('test/screenshots/meter_connect_18a.png')
                    .click('button#manual-toggle')
                    // There is a fade-in animation after the collapse-in elements. That's the reason we need to wait
                    .waitForSelector('#btn-save:visible')
                    .screenshot('test/screenshots/meter_connect_19.png')
                    .expect(['A', 'B'].map(function(letter) {
                        return { selector: 'button[data-target="#content_option' + letter + '"]', visible: true };
                    }));
            });

            it('should allow fuse box meter type to be set to "Whole Home"', function() {
                return browser
                    .screenshot('test/screenshots/meter_connect_20a.png')
                    .expect([
                        { selector: 'button[data-target="#content_optionA"]', visible: true },
                        { selector: 'button[data-target="#content_optionB"]', visible: true },
                        { selector: 'select.usage-type > option[value="home"]',  exists: true },
                        { selector: 'select.usage-type > option[value="solar"]', exists: true }
                    ])
                    .select('select.usage-type', 'home')
                    .screenshot('test/screenshots/meter_connect_20b.png')
                    .expect([
                        { selector: '.usage-type :selected', text: 'Whole Home' },
                        { selector: '#btn-save',             visible: true }
                        // Cannot click save in mock-land, because the smart meter
                        // DB entry will not have been added.
                    ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe('Completed fuse box meter user', function() {
            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/meter-connect', 'onboard23', 'onboard23')
                    .visible('#meter-connect-pane').should.finally.be.true();
            });

            it('should show the meter setup completion page for an onboarded fuse box meter user', function() {
                return browser
                    .screenshot('test/screenshots/meter_connect_21.png')
                    .expect([
                        { selector: '#meter-connect-pane',      visible: true },
                        { selector: '.table-tr',                count: 1 },
                        { selector: '#fusebox-message-content', text: function(t) {
                            var required_messages = [
                                'Your fuse box meter has been configured.',
                                'To check or change the configuration, click on the meter listing below to return to the configuration page.',
                                'All done with setting up your meter? Click on the "Home" button at top left of the screen to return to the home screen.'
                            ];
                            required_messages.forEach(function(message) {
                                t.should.containEql(message);
                            });
                        }}
                    ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe('Completed smart meter user', function() {
            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/meter-connect', 'fred', 'embertec123')
                    .visible('#meter-connect-pane').should.finally.be.true();
            });

            it('should show the meter setup completion page for an onboarded smart meter user', function() {
                // Note that the only way that this can be reached in real life
                // is via entering the URL directly (or perhaps via back button
                // and reloading). There is no nav to this case once the
                // smart meter has been connected.
                return browser
                    .screenshot('test/screenshots/meter_connect_22.png')
                    .expect([
                        { selector: '#meter-connect-pane', visible: true },
                        { selector: '.had-table',          visible: false },
                        { selector: '#smart_meter_done',   text: function(t) {
                            var required_messages = [
                                'A smart meter has been configured.',
                                'Click on the "Home" button at top left of the screen to return to the home screen.'
                            ];
                            required_messages.forEach(function(message) {
                                t.should.containEql(message);
                            });
                        }}
                    ]);
            });
        });

        after(function() {
            return browser
                .do(function(done) {
                    client.end();
                    done();
                }).close();
        });
    });
    /*jshint +W071 */
}());
