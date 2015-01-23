#!/bin/sh
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2012, 2013, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FLOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

# This shell script generates keys and certificates for testing the SSL
# capabilities of MySQL Connector/Python.
#
# Usage:
#  shell> sh generate.sh [destination_folder]
#
DAYS=3306
OU="MySQLConnectorPython"
DESTDIR="."

OPENSSL=`which openssl`
if [ $? -ne 0 ]; then
    echo "openssl not found. Please make sure openssl is in your PATH."
    exit 1
fi

# Destination directory for generate files
if [ "$1" != "" ]; then
    DESTDIR=$1
fi
if [ ! -d $DESTDIR ]; then
    echo "Need a valid destination directory for generated files."
    exit 2
fi

echo
echo "Generating Root Certificate"
echo
$OPENSSL genrsa 2048 > $DESTDIR/tests_CA_key.pem
if [ $? -ne 0 ]; then
    exit 3
fi
SUBJ="/OU=$OU Root CA/CN=MyConnPy Root CA"
$OPENSSL req -batch -new -x509 -nodes -days $DAYS -subj "$SUBJ" \
    -key $DESTDIR/tests_CA_key.pem -out $DESTDIR/tests_CA_cert.pem
if [ $? -ne 0 ]; then
    exit 3
fi

# MySQL Server Certificate: generate, remove passphrase, sign
echo
echo "Generating Server Certificate"
echo
SUBJ="/OU=$OU Server Cert/CN=localhost"
$OPENSSL req -batch -newkey rsa:2048 -days $DAYS -nodes -subj "$SUBJ" \
    -keyout $DESTDIR/tests_server_key.pem -out $DESTDIR/tests_server_req.pem
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL rsa -in $DESTDIR/tests_server_key.pem \
    -out $DESTDIR/tests_server_key.pem
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL x509 -req -in $DESTDIR/tests_server_req.pem -days $DAYS \
    -CA $DESTDIR/tests_CA_cert.pem -CAkey $DESTDIR/tests_CA_key.pem \
    -set_serial 01 -out $DESTDIR/tests_server_cert.pem
if [ $? -ne 0 ]; then
    exit 3
fi

# MySQL Client Certificate: generate, remove passphase, sign
echo
echo "Generating Client Certificate"
echo
SUBJ="/OU=$OU Client Cert/CN=localhost"
$OPENSSL req -batch -newkey rsa:2048 -days $DAYS -nodes -subj "$SUBJ" \
    -keyout $DESTDIR/tests_client_key.pem -out $DESTDIR/tests_client_req.pem
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL rsa -in $DESTDIR/tests_client_key.pem \
    -out $DESTDIR/tests_client_key.pem
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL x509 -req -in $DESTDIR/tests_client_req.pem -days $DAYS \
    -CA $DESTDIR/tests_CA_cert.pem -CAkey $DESTDIR/tests_CA_key.pem \
    -set_serial 01 -out $DESTDIR/tests_client_cert.pem
if [ $? -ne 0 ]; then
    exit 3
fi

# Clean up
echo
echo "Cleaning up"
echo
(cd $DESTDIR; rm tests_server_req.pem tests_client_req.pem)

