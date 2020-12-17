#!/usr/bin/env sh
#i tried it and working like charm just have to note make the file .sh chmod +x and you may need sudo to run with permission but be carefull with sudo
#be sure the $JAVA_HOME is configure correctly or make it static as commentedline 7 below
OLDDIR="$PWD"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
if [ -z "$CACERTS_FILE" ]; then
# you should have java home configure to point for example /usr/lib/jvm/default-java/jre/lib/security/cacerts
    CACERTS_FILE=$JAVA_HOME/lib/security/cacerts
fi

mkdir /tmp/rds-ca && cd /tmp/rds-ca

echo "Downloading RDS certificates..."

curl https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem > rds-combined-ca-bundle.pem

csplit -sk rds-combined-ca-bundle.pem "/-BEGIN CERTIFICATE-/" "{$(grep -c 'BEGIN CERTIFICATE' rds-combined-ca-bundle.pem | awk '{print $1 - 2}')}"

for CERT in xx*; do
    # extract a human-readable alias from the cert
    ALIAS=$(openssl x509 -noout -text -in $CERT |
                   perl -ne 'next unless /Subject:/; s/.*CN=//; print')
    echo "importing $ALIAS"
    # import the cert into the default java keystore
    keytool -import \
            -keystore  $CACERTS_FILE \
            -storepass changeit -noprompt \
            -alias "$ALIAS" -file $CERT
done

cd "$OLDDIR"

rm -r /tmp/rds-ca
