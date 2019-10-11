default: dirs

dirs:
	mkdir -p root/pub
	mkdir -p root/logs
	mkdir -p root/tmp
	mkdir -p root/conf
	chmod 0755 run.sh

clean: 
	rm -f root/tmp/*

ssl: private-key.pem server-cert.pem

private-key.pem:
	openssl genrsa -out private-key.pem 2048

server-cert.pem: private-key.pem
	openssl req -new -x509 -nodes -sha1 -days 365 -key private-key.pem \
		> server-cert.pem
