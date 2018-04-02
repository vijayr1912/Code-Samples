/**
 * @file http_test/011-get-forgotten-password.js
 * This test performs an HTTP GET against the IPM forgotten password page
 */

(function() {
    "use strict";

    // Module dependencies.
    var expect  = require('chai').expect;
    var request = require('supertest-session')('http://127.0.0.1:3000');

    describe("Forgotten password page", function() {
        it("should respond with 200 OK", function(done) {
            request.get('/user/forgotten')
                .expect(200)
                .expect('Content-Type', /text\/html/)
                .end(function(err, res) {
                    expect(err).to.not.exist;
                    expect(res.text).to.have.length.above(1);
                    done();
                });
        });

        it("should not load when logged-in", function(done) {
            // POST the login form
            request.post('/login')
                .send({
                    username: 'fred',
                    password: 'embertec123'
                })
                .expect(302)
                .end(function(err) {
                    expect(err).to.not.exist;

                    request.get('/user/forgotten')
                        .expect(302)
                        .expect('Location', /^\/home$/)
                        .end(function(err, res) {
                            expect(err).to.not.exist;
                            expect(res.text).to.have.length.above(1);
                            done();
                        });
                });
        });
    });
}());
