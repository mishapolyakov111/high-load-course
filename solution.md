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
