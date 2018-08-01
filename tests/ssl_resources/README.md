# MySQL TLS Stuff

This folder contains some resources to be mounted into a mysql container to support TLS
 
Most of the instructions were taken from here https://dev.mysql.com/doc/refman/5.7/en/creating-ssl-files-using-openssl.html

# Generating certificates
```bash
openssl genrsa 2048 > ca-key.pem
openssl req -new -x509 -nodes -days 3600 -key ca-key.pem -out ca.pem

openssl req -newkey rsa:2048 -days 3600 -nodes -keyout server-key.pem -out server-req.pem
openssl rsa -in server-key.pem -out server-key.pem
openssl x509 -req -in server-req.pem -days 3600 -CA ca.pem -CAkey ca-key.pem -set_serial 01 -out server-cert.pem
```
The current files under `ssl/` have the default values provided by openssl.

# MySQL Config
MySQL imports all `.cnf` files under `/etc/mysql/conf.d` so a `tls.cnf` is placed in there referencing the SSL CA 
cert and server cert and key. The entire `ssl/` directory should be mounted on the container to `/etc/mysql/ssl/`
