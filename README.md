# dowgraf
Downloads Grafana panels as images

## Searches into the dashboard database
```
./dowgraf -u USER:PSWD -H HOST -sd -sd KEY_WORD | jq '.[] | .uid'
DASHBOARD_UID
```

## Downloads all panels in a dashboard
```
./dowgraf -u USER:PSWD -H HOST -sp DASHBOARD_UID -tr -tr 'START_TIME_IN_EPOCH:START_TIME_IN_EPOCH[,START_TIME_IN_EPOCH:START_TIME_IN_EPOCH]'
```

