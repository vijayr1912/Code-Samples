/**
 * @file http_test/010-get-homepage.js
 * This test performs an HTTP GET against the IPM homepage
 */

(function() {
    "use strict";

    // Module dependencies.
    var expect  = require('chai').expect;
    var request = require('supertest-session')('http://127.0.0.1:3000');
    var async   = require('async');

    describe("Homepage", function() {
        it("should respond with 200 OK", function(done) {
            request.get('/')
                .expect(200)
                .expect('Content-Type', /text\/html/)
                .end(function(err, res) {
                    expect(err).to.not.exist;
                    expect(res.text).to.have.length.above(1);
                    done();
                });
        });

        it("should be different when logged in", function(done) {
            // forecast algorithm needs more time
            this.timeout(Math.max(this.timeout(), 200000));
            var not_logged_in;
            var logged_in;

            async.waterfall([
                function(callback) {
                    // GET the homepage
                    request.get('/')
                        .expect(200)
                        .expect('Content-Type', /text\/html/)
                        .end(callback);
                }, function(res, callback) {
                    not_logged_in = res.text;

                    // POST the login form
                    request.post('/login')
                        .send({
                            username: 'fred',
                            password: 'embertec123'
                        })
                        .expect(302)
                        .expect('Location', /^\/home$/)
                        .end(callback);
                }, function(res, callback) {
                    // GET the homepage again
                    request.get('/')
                        .expect(302)
                        .expect('Location', /^\/home$/)
                        .end(callback);
                }, function(res, callback) {
                    // Get the homepage for logged-in users
                    request.get('/home')
                        .expect(200)
                        .expect('Content-Type', /text\/html/)
                        .end(callback);
                }, function(res, callback) {
                    logged_in = res.text;
                    expect(logged_in).to.not.equal(not_logged_in);


                    // POST the logout URL
                    request.post('/logout')
                        .expect(302)
                        .expect('Location', /^\/$/)
                        .end(callback);
                }, function(res, callback) {
                    // Try the homepage for logged-in users
                    request.get('/home')
                        .expect(302)
                        .expect('Location', /^\/login$/)
                        .end(callback);
                }, function(res, callback) {
                    // GET the homepage again
                    request.get('/')
                        .expect(200)
                        .expect('Content-Type', /text\/html/)
                        .end(callback);
                }, function(res, callback) {
                    expect(res.text).to.equal(not_logged_in);
                    callback();
                }
            ], function(err) {
                expect(err).to.not.exist;
                done();
            });
        });
    });
}());
