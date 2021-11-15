from campuslibs.loggers.mongo import save_to_mongo
from authorizenet import apicontractsv1
from authorizenet.apicontrollers import *
from decouple import config

def payment_transaction(payment, store_payment_gateway, transaction_type):
    authorizenet_base_api = config('AUTHORIZE_NET_BASE_API', 'https://apitest.authorize.net/xml/v1/request.api')
    merchant_auth = apicontractsv1.merchantAuthenticationType()
    merchant_auth.name = store_payment_gateway.payment_gateway_config.configuration['login_id']
    merchant_auth.transactionKey = store_payment_gateway.payment_gateway_config.configuration['transaction_key']

    transactionrequest = apicontractsv1.transactionRequestType()
    transactionrequest.transactionType = transaction_type
    transactionrequest.amount = payment.amount
    transactionrequest.refTransId = payment.transaction_reference

    createtransactionrequest = apicontractsv1.createTransactionRequest()
    createtransactionrequest.merchantAuthentication = merchant_auth
    createtransactionrequest.refId = str(payment.transaction_request_id)

    createtransactionrequest.transactionRequest = transactionrequest
    createtransactioncontroller = createTransactionController(createtransactionrequest)
    createtransactioncontroller.setenvironment(authorizenet_base_api)
    createtransactioncontroller.execute()

    response = createtransactioncontroller.getresponse()

    if (response.messages.resultCode=="Ok"):
        status_data={'type': 'payment', 'payment_status': 'success', 'transaction_id': response.transactionResponse.transId, 'description': response.transactionResponse.messages.message[0].description, 'transaction_type': transaction_type}
        save_to_mongo(data=status_data, collection='enrollment_status_history')
    else:
        status_data={'type': 'payment', 'payment_status': 'failed', 'payment_id': str(payment.id), 'transaction_type': transaction_type}
        save_to_mongo(data=status_data, collection='enrollment_status_history')
