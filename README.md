# think_thrift
Thrift middleware for thinkkoa.


# usage

```
const rpc = require("think_thrift");
const RPCService = require('../../gen-nodejs/RPCService');

await rpc("method", {
    aa: 1,
    bb: 2
}, RPCService, {
    host: '127.0.0.1',
    port: 8090,
    max_attempts: 3,
    connect_timeout: 2000
});
```