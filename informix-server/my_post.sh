useradd -d /home/ifxsurr -s /bin/sh ifxsurr
mkdir /etc/informix
echo "USERS:ifxsurr" > /etc/informix/allowed.surrogates
chown root:root /etc/informix/allowed.surrogates
chmod 644 /etc/informix/allowed.surrogates