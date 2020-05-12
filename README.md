# JMAP proxy
This is JMAP server with IMAP backend programmed in python asyncio.
ASGI server 

This is an implementation of a proxy server for the JMAP protocol as specified at http://jmap.io/

At the backend, it talks to IMAP and SMTP servers to allow placing a JMAP interface on top of a legacy mail system.

For efficiency reasons, this initial implementation requires that all servers support the CONDSTORE extension, (RFC4551/RFC7162).

# Run

    uvicorn api:app --host 0.0.0.0 --port 5000 --loop uvloop --log-level info --workers 1

# Thanks
https://github.com/jmapio/jmap-perl
