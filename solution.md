параметры системы оплаты (acc-12):

PaymentAccountProperties(
    serviceName=m3402-SkibidiToilets,
    accountName=acc-12,
    parallelRequests=20000,
    rateLimitPerSec=1100,
    price=30,
    averageProcessingTime=PT10S,
    enabled=true)


Тест:
{
    "ratePerSecond": 1000,
    "testCount": 200000,
    "processingTimeMillis": 50000
}


успешный [прогон](http://77.234.215.138:33000/d/KVr-Vmpnz/services-statistic?orgId=1&from=2025-12-11T17:43:59.000Z&to=2025-12-11T17:48:32.604Z&timezone=browser&var-service=m3402-SkibidiToilets&refresh=5s) 
