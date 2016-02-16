# XXG7Wctz30qN6n5xUHKH2g
This program gets currency exchange rates from currencylayer.com
It uses the free api so it is only capable of converting from USD to other
currencies.

Usage:
-s option starts the seed only
-w option starts the worker

Examples:
To start the initial seed and a single worker:
node exchange.js -s -w
To start additional workers:
node exchange.js -w
