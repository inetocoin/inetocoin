import hashlib
import json
import time
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class Blockchain:
    def __init__(self):
        self.chain = []
        self.transactions = []
        self.create_block(proof=1, previous_hash='0')

    def create_block(self, proof, previous_hash):
        block = {
            'index': len(self.chain) + 1,
            'timestamp': time.time(),
            'transactions': self.transactions,
            'proof': proof,
            'previous_hash': previous_hash
        }
        self.transactions = []
        self.chain.append(block)
        return block

    def get_previous_block(self):
        return self.chain[-1]

    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False
        while not check_proof:
            hash_operation = hashlib.sha256(str(new_proof**2 - previous_proof**2).encode()).hexdigest()
            if hash_operation[:4] == '0000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    def add_transaction(self, sender, receiver, amount):
        self.transactions.append({'sender': sender, 'receiver': receiver, 'amount': amount})
        return self.get_previous_block()['index'] + 1

class SmartContract:
    def __init__(self):
        self.contracts = {}

    def create_contract(self, contract_id, conditions):
        if contract_id in self.contracts:
            return "Contract already exists"
        self.contracts[contract_id] = {
            'conditions': conditions,
            'executed': False
        }
        return "Contract created successfully"

    def execute_contract(self, contract_id, context):
        if contract_id not in self.contracts:
            return "Contract not found"
        contract = self.contracts[contract_id]
        if contract['executed']:
            return "Contract already executed"

        if all(context.get(k) == v for k, v in contract['conditions'].items()):
            contract['executed'] = True
            return "Contract executed successfully"
        else:
            return "Contract conditions not met"

app = FastAPI()
blockchain = Blockchain()
smart_contracts = SmartContract()

class Transaction(BaseModel):
    sender: str
    receiver: str
    amount: float

class Contract(BaseModel):
    contract_id: str
    conditions: dict

class ExecutionContext(BaseModel):
    contract_id: str
    context: dict

@app.get('/v1/mine_block')
def mine_block():
    previous_block = blockchain.get_previous_block()
    previous_proof = previous_block['proof']
    proof = blockchain.proof_of_work(previous_proof)
    previous_hash = blockchain.hash(previous_block)
    block = blockchain.create_block(proof, previous_hash)
    return {'message': 'New block mined successfully!', 'block': block}

@app.post('/v1/add_transaction')
def add_transaction(transaction: Transaction):
    index = blockchain.add_transaction(transaction.sender, transaction.receiver, transaction.amount)
    return {'message': f'Transaction added to block {index}'}

@app.get('/v1/get_chain')
def get_chain():
    return {'chain': blockchain.chain, 'length': len(blockchain.chain)}

@app.post('/v1/create_contract')
def create_contract(contract: Contract):
    result = smart_contracts.create_contract(contract.contract_id, contract.conditions)
    return {'message': result}

@app.post('/v1/execute_contract')
def execute_contract(execution: ExecutionContext):
    result = smart_contracts.execute_contract(execution.contract_id, execution.context)
    return {'message': result}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
