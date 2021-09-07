from authorizenet import apicontractsv1
from authorizenet.apicontrollers import *

def capture_payment(payment, store_payment_gateway):
    authorizenet_base_api = 'https://apitest.authorize.net/xml/v1/request.api'
    merchant_auth = apicontractsv1.merchantAuthenticationType()
    merchant_auth.name = store_payment_gateway.payment_gateway_config.configuration['login_id']
    merchant_auth.transactionKey = store_payment_gateway.payment_gateway_config.configuration['transaction_key']

    transactionrequest = apicontractsv1.transactionRequestType()
    transactionrequest.transactionType = "priorAuthCaptureTransaction"
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
        print(f'Transaction ID : {response.transactionResponse.transId}') 
        print(response.transactionResponse.messages.message[0].description)
    else:
        print(f'response code: {response.messages.resultCode}')