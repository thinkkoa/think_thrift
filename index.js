/**
 * @ author: richen
 * @ copyright: Copyright (c) - <richenlin(at)gmail.com>
 * @ license: MIT
 * @ version: 2020-05-19 11:30:33
 */

const thrift = require('thrift');
const helper = require('think_lib');
const logger = require('think_logger');
var _THRIFT_CONNECTION = {};
var _THRIFT_CLIENTS = {};
const defaultOptions = {
    host: '127.0.0.1',
    port: 8090,
    max_attempts: 3,
    connect_timeout: 2000
};

/**
 * create conn
 *
 * @param {*} options
 * @param {*} keys
 * @returns
 */
const connect = function (options) {
    options = Object.assign(defaultOptions, options);
    const keys = helper.murmurHash(`${options.host}_${options.port}`);
    if (_THRIFT_CONNECTION[keys]) {
        return Promise.resolve(_THRIFT_CONNECTION[keys]);
    }

    const rpcConnection = thrift.createConnection(options.host, options.port, options);
    const defer = helper.defer();
    rpcConnection.on('error', function (err) {
        rpcConnection.end();
        _THRIFT_CONNECTION[keys] = null;

        logger.error(err.stack);
        defer.reject(err);
    });
    rpcConnection.on('connect', function () {
        logger.info('thrift server connected.');
        _THRIFT_CONNECTION[keys] = { keys: keys, conn: rpcConnection };
        defer.resolve(_THRIFT_CONNECTION[keys]);
    });
    return defer.promise;
};

/**
 *
 *
 * @param {*} method rpc method
 * @param {*} params 
 * @param {*} service  require('../../gen-nodejs/RPCService');
 * @param {*} [options={}]
 * @returns
 */
module.exports = function (method, params, service, options = {}) {
    return connect(options).then(rpc => {
        if (rpc.keys && rpc.conn) {
            if (!_THRIFT_CLIENTS[rpc.keys]) {
                _THRIFT_CLIENTS[rpc.keys] = thrift.createClient(service, rpc.conn);
            }
            if (_THRIFT_CLIENTS[rpc.keys] && _THRIFT_CLIENTS[rpc.keys][method]) {
                const deferred = helper.getDefer();
                _THRIFT_CLIENTS[rpc.keys][method](JSON.stringify(params), (err, response) => {
                    if (err) {
                        deferred.reject({ code: 500, message: err.message || 'RPC error' });
                    }
                    deferred.resolve(response);
                });
                return deferred.promise;
            } else {
                return Promise.reject({ code: 502, message: `RPC method ${method} is undefined.` });
            }
        } else {
            return Promise.reject({ code: 503, message: 'RPC connection error' });
        }
    });
};