# masql

mask sql file such as dumped queries with mysqldump

## example

```sh
# ref: https://dev.mysql.com/doc/index-other.html
curl https://downloads.mysql.com/docs/world-db.tar.gz
tar -xf world-db.tar.gz
cd world-db
# Replace the third column in the county table with the string "xxxx"
cat world.sql | masql --replace='country:3:"xxxx"' > masked.sql
```

## build

```sh
git clone https://github.com/yammerjp/masql.git
cd masql
go build -o masql main.go
# If you want to install it on your system
sudo cp masql /usr/local/bin/masql
sudo chmod 755 /usr/local/bin/masql
```
