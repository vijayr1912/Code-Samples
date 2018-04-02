/**
 * @file electricity-bill-breakdown/index.js
 * Functional tests for the electricity-bill page
 */

(function() {
    "use strict";

    //Module dependencies
    var horseman = require('test/lib/browser/horseman.js');
    var moment   = require('moment');

    function checkVisible(selector) {
        // NOTE: JQuery deems an object as visible iff it takes up space
        // within the document.
        // This means that elements inside an SVG are considered invisible!
        return $(selector).is(':visible');
    }

    function checkExists(selector) {
        return $(selector).length > 0;
    }

    describe('Electricity bill breakdown page', function(){
        this.timeout(Math.max(this.timeout(), 10000));
        var browser;

        before(function() {
            browser = horseman.browserInit();
            return browser;
        });

        beforeEach(function() {
            return browser.log(this.currentTest.title);
        });

        it('should require a login', function() {
            return browser.login('http://127.0.0.1:3000/electricity-bill', 'fred', 'embertec123');
        });

        it('should only show the bill summary pane', function() {
            return browser
                .screenshot('test/screenshots/electricity_bill_breakdown_1.png')
                .expect([
                    {selector: '#electricity-bill-breakdown-graph', exists: false},
                    {selector: '#bill-cost-breakdown', visible: true},
                    {selector: 'title', text: 'My Bill Breakdown - Emberpulse'}
                ]);
        });

        // The following cases are disabled because the energy breakdown feature
        // is currently turned off.
        it.skip('should be available after login', function() {
            return browser
                .screenshot('test/screenshots/electricity_bill_breakdown_1.png')
                .expect([
                    { selector: '#electricity-bill-breakdown-pane', visible: true },
                    { selector: 'title',                            text: 'My Energy Usage Breakdown - Emberpulse' }
                ]);
        });

        it.skip('should show last week by default', function() {
            return browser
                .evaluate(function() {
                    var mmfmt = 'DD/MM/YYYY';
                    var start = moment().startOf('week');
                    window.ipm.tick.webkitMomentDSTHourFix(start, 0);
                    start.subtract(1, 'week');
                    window.ipm.tick.webkitMomentDSTHourFix(start, 0);
                    var end = start.clone().add(1, 'week').subtract(1, 'second');
                    window.ipm.tick.webkitMomentDSTHourFix(end, 23);
                    return start.format(mmfmt) + ' - ' + end.format(mmfmt);
                })
                .then(function(last_week) {
                    return browser.text('#breakdown-nav span.period-name').should.finally.equal(last_week);
                });
        });

        describe.skip('with no appliance metrics and no tariff metrics', function(){

            // Note that unfortunately we cannot query svg g elements for visiblity
            // in phantomjs, so can't check for (in)visibility of the pie chart. :-(

            it('should show appropriate messages for empty data case', function() {
                return browser
                    .expect([
                        { selector: 'text.nv-noData', exists: true },
                        { selector: 'text.nv-noData', text: 'Sorry, not enough data' },
                        { selector: '.summary legend', text: 'Total Energy' },
                        { selector: 'label.kwh.active', visible: true },
                        { selector: 'label.cost.active', visible: false },
                        { selector: '.readout span.decimal', visible: true },
                        { selector: '.readout span.decimal', text: '--' },
                        { selector: '.readout span.units.post', visible: true },
                        { selector: '.readout span.units.post', text: 'kWh' },
                        { selector: '.readout span.units.pre', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: false },
                        { selector: '#compare-bar', exists: false }
                    ])
                    .click('label.cost')
                    .waitForSelector('label.cost.active')
                    .screenshot('test/screenshots/electricity_bill_breakdown_2.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: true },
                        { selector: 'text.nv-noData', text: 'Sorry, not enough data' },
                        { selector: '.summary legend', text: 'Total Cost' },
                        { selector: 'label.kwh.active', visible: false },
                        { selector: 'label.cost.active', visible: true },
                        { selector: '.readout span.decimal', visible: true},
                        { selector: '.readout span.decimal', text: '--' },
                        { selector: '.readout span.units.pre', visible: true },
                        { selector: '.readout span.units.pre', text: '$' },
                        { selector: '.readout span.units.post', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: false },
                        { selector: '#compare-bar', exists: false }
                    ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe.skip('with energy metrics present but no tariff metrics', function(){

            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/electricity-bill', 'onboard20', 'onboard20')
                    .visible('label.kwh.active').should.finally.be.true();
            });

            it('should show appropriate messages for cost and data for energy', function() {
                return browser.screenshot('test/screenshots/electricity_bill_breakdown_3.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: false },
                        { selector: '.summary legend', text: 'Total Energy' },
                        { selector: 'label.kwh.active', visible: true },
                        { selector: 'label.cost.active', visible: false },
                        { selector: '.readout span.decimal', visible: true},
                        { selector: '.readout span.decimal', text: '39' },
                        { selector: '.readout span.units.post', visible: true },
                        { selector: '.readout span.units.post', text: 'kWh' },
                        { selector: '.readout span.units.pre', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' },
                        { selector: '#compare-bar', exists: false }
                    ])
                    .click('label.cost')
                    .waitForSelector('label.cost.active')
                    .screenshot('test/screenshots/electricity_bill_breakdown_4.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: true },
                        { selector: 'text.nv-noData', text: 'Sorry, not enough data' },
                        { selector: '.summary legend', text: 'Total Cost' },
                        { selector: 'label.kwh.active', visible: false },
                        { selector: 'label.cost.active', visible: true },
                        { selector: '.readout span.decimal', visible: true},
                        { selector: '.readout span.decimal', text: '--' },
                        { selector: '.readout span.units.pre', visible: true },
                        { selector: '.readout span.units.pre', text: '$' },
                        { selector: '.readout span.units.post', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' },
                        { selector: '#compare-bar', exists: false }
                    ]);
            });


            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        describe.skip('with energy and tariff metrics present', function(){

            before(function() {
                return browser
                    .login('http://127.0.0.1:3000/electricity-bill', 'wilma', 'embertec999')
                    .visible('label.cost.active').should.finally.be.true();
            });

            it('should show appropriate data', function() {
                return browser.screenshot('test/screenshots/electricity_bill_breakdown_5.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: false },
                        { selector: '.summary legend', text: 'Total Cost' },
                        { selector: 'label.cost.active', visible: true },
                        { selector: 'label.kwh.active', visible: false },
                        { selector: '.readout span.decimal', visible: true},
                        { selector: '.readout span.decimal', text: '2' },
                        { selector: '.readout span.units.pre', visible: true },
                        { selector: '.readout span.units.pre', text: '$' },
                        { selector: '.readout span.units.post', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' }
                    ])
                    .click('label.kwh')
                    .waitForSelector('label.kwh.active')
                    .screenshot('test/screenshots/electricity_bill_breakdown_6.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: false },
                        { selector: '.summary legend', text: 'Total Energy' },
                        { selector: 'label.cost.active', visible: false },
                        { selector: 'label.kwh.active', visible: true },
                        { selector: '.readout span.decimal', visible: true},
                        { selector: '.readout span.decimal', text: '39' },
                        { selector: '.readout span.units.post', visible: true },
                        { selector: '.readout span.units.post', text: 'kWh' },
                        { selector: '.readout span.units.pre', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' }
                    ]);
            });

            it('should allow selection of a different time range', function() {
                var last_year = moment().startOf('year').subtract(1, 'week').year().toString();
                return browser
                    .click('label.cost')
                    .waitForSelector('label.cost.active')
                    .click('a.lastyear')
                    .waitFor(function() {
                        return $('.breakdown-time-menu > span.period-name').text();
                    }, last_year)
                    .screenshot('test/screenshots/electricity_bill_breakdown_7.png');
            });

            it('should show correct data for a different time range', function() {
                return browser.screenshot('test/screenshots/electricity_bill_breakdown_8.png')
                    .click('label.cost')
                    .waitForSelector('label.cost.active')
                    .waitFor(checkExists, 'text.nv-noData', false)
                    .expect([
                        { selector: 'text.nv-noData', exists: false },
                        { selector: '.summary legend', text: 'Total Cost' },
                        { selector: 'label.cost.active', visible: true },
                        { selector: 'label.kwh.active', visible: false },
                        { selector: '.readout span.decimal', visible: true},
                        // Allow for leap years.
                        { selector: '.readout span.decimal', text: /^(?:5329|5344)$/ },
                        { selector: '.readout span.units.pre', visible: true },
                        { selector: '.readout span.units.pre', text: '$' },
                        { selector: '.readout span.units.post', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' }
                    ])
                    .click('label.kwh')
                    .waitForSelector('label.kwh.active')
                    .screenshot('test/screenshots/electricity_bill_breakdown_9.png')
                    .expect([
                        { selector: 'text.nv-noData', exists: false },
                        { selector: '.summary legend', text: 'Total Energy' },
                        { selector: 'label.cost.active', visible: false },
                        { selector: 'label.kwh.active', visible: true },
                        { selector: '.readout span.decimal', visible: true},
                        // Allow for leap years.
                        { selector: '.readout span.decimal', text: /^(?:106580|106872)$/ },
                        { selector: '.readout span.units.post', visible: true },
                        { selector: '.readout span.units.post', text: 'kWh' },
                        { selector: '.readout span.units.pre', visible: false },
                        { selector: 'svg', visible: true },
                        { selector: '.key li', visible: true },
                        { selector: '.key li:nth-child(1)', text: 'Lights' },
                        { selector: '.key li:nth-child(2)', text: 'Home Entertainment' },
                        { selector: '.key li:nth-child(3)', text: 'Always On' }
                    ]);
            });

            // A few other cases missing here but limited to the clumsy
            // phantomjs capability:
            // 1. compare-bar hidden when scrolling and changing period duration
            // 2. test with a tip message (need the pg mockup db)
            it('should show compare bar when clicking a pie slice', function() {
                return browser
                    .evaluate(function() {
                        jQuery.fn.d3Click = function() {
                            this.each(function(i, e) {
                                var evt = document.createEvent('CustomEvent');
                                evt.initCustomEvent('click', false, false, null);
                                e.dispatchEvent(evt);
                            });
                        };
                    })
                    .expect({ selector: '#compare-bar', visible: false })
                    .click('label.cost')
                    .waitForSelector('label.cost.active')
                    .evaluate(function() { $('#graph-parts svg g.nv-slice:nth-child(1)').d3Click(); })
                    .waitForSelector('#compare-bar')
                    .screenshot('test/screenshots/electricity_bill_breakdown_a.png')
                    .expect([
                        { selector: '#compare-bar .ec-bar', visible: true },
                        { selector: '#compare-bar .ec-tip', visible: false }
                    ]);
            });

            it('should keep the compare bar when switching to different unit', function() {
                return browser
                    .click('label.kwh')
                    .waitForSelector('label.kwh.active')
                    .screenshot('test/screenshots/electricity_bill_breakdown_b.png')
                    .expect([
                        { selector: '#compare-bar .ec-bar', visible: true },
                        { selector: '#compare-bar .ec-tip', visible: false }
                    ]);
            });

            after(function() {
                return browser.logout().title().should.finally.equal('Welcome to Emberpulse');
            });
        });

        after(function() {
            return browser.close();
        });
    });
}());
