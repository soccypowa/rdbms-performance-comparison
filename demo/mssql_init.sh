echo "executing..."
for i in {1..60};
do
    /opt/mssql-tools/bin/sqlcmd -S "tcp:localhost,1434" -U SA -P "myStrong(!)Password" -d master -i /mssql_init.sql
    if [ $? -eq 0 ]
    then
        echo "mssql_init.sql completed"
        break
    else
        echo "not ready yet..."
        sleep 1
    fi
done