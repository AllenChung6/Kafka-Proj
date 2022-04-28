from kafka import KafkaConsumer, TopicPartition
from json import loads
import numpy as np
from statistics import pstdev


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        self.deposits = []
        self.withdrawals = []
        # add a way to connect to your database here.

        # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.deposits.append(message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.withdrawals.append(message['amt'])
            #print(self.custBalances)
            # Add all deposits together and withdrawals together.
            print(f'The mean of all deposits is: ' + str(np.mean(self.deposits)))
            print(f'The mean of all withdrawals is: ' + str(np.mean(self.withdrawals)))
            print(f'The standard deviation of all deposits is: ' + str(pstdev(self.deposits)))
            if len(self.withdrawals) == 0:
                print('No standard deviation for withdrawals')
            else:
                print(f'The standard deviation of all withdrawals is: ' + str(pstdev(self.withdrawals)))


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()