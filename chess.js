const express = require('express');
const app = express();
const redis = require("ioredis");
const redisclient = new redis() 
const bodyParser = require('body-parser');
const PORT = process.env.PORT || 8001;

// Middleware to parse JSON bodies
app.use(bodyParser.json());

console.log("Connecting to Redis...");

redisclient.on("connect", () => { 
    console.log("Connected to Redis!"); 
}); 

// Function to add client information to Redis
async function addClientInfo(clientInfo) {
    const key = clientInfo.ClientID;
    const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
    console.log('hello');
    
    try {
        //Sending data to redis datastore
        await redisclient.hset(hashname, key, JSON.stringify(clientInfo));
        console.log('ClientInfo added successfully to Redis');
    } catch (error) {
        console.error('Error adding client information to Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// Function to update client information to Redis

async function updateClientInfo(clientInfo) {
    const key = clientInfo.ClientID;
    const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
    console.log('hi');
    
    try {
        //Updating data to redis datastore
        await redisclient.hset(hashname, key, JSON.stringify(clientInfo));
        console.log('ClientInfo updated successfully');
    } catch (error) {
        console.error('Error updating client information:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

//  Function to get client information from Redis

async function getClientInfo(clientInfo){
    const key = clientInfo.ClientID;
    const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
    console.log('Fetching client info from redis');
    try {
        //Getting client data from redis datastore
        const reply = await redisclient.hget(hashname, key);
        if (!reply) {
            throw new Error('Client info not found');
        }
        const clientInfo = JSON.parse(reply);
        console.log('hu');
        console.log('ClientInfo retrieved successfully');
        return clientInfo;
    } catch (error) {
        console.error('Error getting client information from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}


// Function to retrieve client information from Redis

async function removeClientInfo(clientInfo) {
    const key = clientInfo.ClientID;
    const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
    console.log('hiswap');
    console.log('Removing client info from Redis...');
    
    try {
        //removing client data from redis datastore
        const reply = await redisclient.del(hashname, key);
        if (!reply) {
            throw new Error('Client info not found');
        }
        console.log('ClientInfo removed successfully');
        return { message: 'ClientInfo removed successfully' };
    } catch (error) {
        console.error('Error removing client information from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

//Function to retrieve all client information from Redis

async function getAllClientInfo() {
    console.log('Fetching all client info from Redis...');
    try {
        // Fetch all hash names matching the pattern 'TenantId_OMSid'
        const hashNames = await redisclient.scan(0, 'MATCH', '*_*'); // Adjust the pattern as needed

        // Fetch client info for each hash name
        const allClientInfo = [];
        for (const hashName of hashNames[1]) {
            const clientInfoEntries = await redisclient.hgetall(hashName);
            // Convert each client info entry to JSON object and push to the result array
            for (const key in clientInfoEntries) {
                allClientInfo.push(JSON.parse(clientInfoEntries[key]));
            }
        }

        console.log('All client info retrieved successfully from Redis');
        return allClientInfo;
    } catch (error) {
        console.error('Error getting all client information from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// POST endpoint for ClientInfo

app.post('/clientInfo', async(req, res) => {
    const { OperationType } = req.body;
    // Validating Message Type
    const mt = req.body;
    const mtr = mt.MsgType;
    if(mtr!=1121){
        res.send('message type wrong');
    }
    switch (OperationType) {
        case 100: // Add Client
        try {
            const clientInfo = req.body;
            const key = clientInfo.ClientID;
    const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
    // Validating Client Id
    if( await redisclient.hexists(hashname,key)==0){
           addClientInfo(req.body);//Invoking addClientInfo
            console.log('ClientInfo added successfully');
            return res.json({ message: 'ClientInfo added successfully' });
    }
    else{
        res.send('Already exists');
        
    }
        } catch (error) {
            console.error('Error adding client information to Redis:', error);
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        
            break;
        case 101: // Update Client
        try{
            const clientInfo = req.body;
            const key = clientInfo.ClientID;
            const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
            // Validating Client Id
            if(redisclient.hexists(hashname,key)==1){
            updateClientInfo(req.body);//Invoking UpdateClientInfo
            console.log('ClientInfo updated successfully');
            return res.json({ message: 'ClientInfo updated successfully' });
            }
            else{
                res.send('Key does not exist');
            }
        }
        catch(error){
            console.error('Error updating client information to Redis:', error); 
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        break;
           
       
            case 102: // Remove Client
    try {
        const clientInfo = req.body;
            const key = clientInfo.ClientID;
            const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
            // Validating Client Id
            if(redisclient.hexists(hashname,key)==1){
            removeClientInfo(req.body);//Invoking removeClientInfo
            
        console.log('ClientInfo removed successfully from Redis');
        return res.json({ message: 'ClientInfo removed successfully' });
            }
            else{
                res.send('Key does not exist');
            }
    } catch (error) {
        console.error('Error removing client information from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;

   
    case 103: // Get Client
    try {
        const clientInfo = req.body;
        const key = clientInfo.ClientID;
        const hashname = clientInfo.TenantId + "_" + clientInfo.OMSid;
        // Validating Client Id
if(redisclient.hexists(hashname,key)==1){
        const fetchedClientInfo = await getClientInfo(clientInfo);//Invoking getClientInfo
        console.log('ClientInfo retrieved successfully from Redis');
        return res.json(fetchedClientInfo);
        }
        else{
            res.send('Key does not exist');  
        }
    } catch (error) {
        console.error('Error getting client information from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;
    case 104: // Get All Client
    try {
        const allClientInfo = await getAllClientInfo();
        console.log('All client info retrieved successfully from Redis');
        return res.json(allClientInfo);
    } catch (error) {
        console.error('Error getting all client information from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;


        default:
            res.status(400).json({ error: 'Invalid OperationType' });
    }
});

//************************************************************************************************
//************************************************************************************************


// Function to add order information to Redis

async function addorderInfo(orderInfo) {
    const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
    console.log('hello');
     try {
        await redisclient.hset(hashn, ke, JSON.stringify(orderInfo));//Adding order info to redis
        console.log('orderInfo added successfully to Redis');
    } catch (error) {
        console.error('Error adding client information to Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// Function to update order information to Redis

async function updateorderInfo(orderInfo) {
    const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
    console.log('hi');
    
    try {
        await redisclient.hset(hashn, ke, JSON.stringify(orderInfo));//Updating order info to redis
        console.log('orderInfo updated successfully');
    } catch (error) {
        console.error('Error updating order information:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// Function to add get order information from Redis

async function getorderInfo(orderInfo){
    const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
    console.log('Fetching order info from redis');
    try {
        const reply = await redisclient.hget(hashn, ke); //Getting order info to redis
        if (!reply) {
            throw new Error('order info not found');
        }
        const clientInfo = JSON.parse(reply);
        console.log('hu');
        console.log('OrderInfo retrieved successfully');
        return clientInfo;
    } catch (error) {
        console.error('Error getting order information from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// Function to add remove order information from Redis

async function removeorderInfo(orderInfo) {
    const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
    console.log('hiswap');
    console.log('Removing order info from Redis...');
    
    try {
        const reply = await redisclient.del(hashn, ke);  //Removing order info to redis
        if (!reply) {
            throw new Error('order info not found');
        }
        console.log('orderInfo removed successfully');
        return { message: 'orderInfo removed successfully' };
    } catch (error) {
        console.error('Error removing order information from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// Function to add get all orders information of client from Redis

async function getAllOrderInfoForClient(clientID, tenantID, OMSID, token) {
    console.log('Fetching all order info for client from Redis...');
    const hashn = `${tenantID}_${OMSID}_${clientID}_${token}`;
    try {
        // Check if the hash exists for the given clientID
        if (await redisclient.exists(hashn)) {
            // Fetch all order information for the clientID
            const orderInfoEntries = await redisclient.hgetall(hashn);

            // Convert each order info entry to JSON object and push to the result array
            const allOrderInfo = Object.values(orderInfoEntries).map(JSON.parse);

            console.log('All order info for client retrieved successfully from Redis');
            return allOrderInfo;
        } else {
            // If the hash doesn't exist, return an empty array
            console.log('No order info found for the client in Redis');
            return [];
        }
    } catch (error) {
        console.error('Error getting all order information for client from Redis:', error);
        throw error; // Re-throw the error to handle it further up the call stack
    }
}

// POST endpoint for OrderInfo

app.post('/orderInfo', async(req, res) => {
    const { OperationType } = req.body;
    //Validating Message Type
    const mt = req.body;
    const mtr = mt.MsgType;
    if(mtr!=1120){
        res.send('message type wrong');
    }

    switch (OperationType) {
        case 100: // Add order
        try {
            const orderInfo = req.body;
            const key = orderInfo.ClientID;
            const hashname = orderInfo.TenantId + "_" + orderInfo.OMSid;
            const ke = orderInfo.OrderID;
            const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
            //Validating Client Id
       if(await redisclient.hexists(hashname,key)==1){
        // Validating Order Id
        if(await redisclient.hexists(hashn,ke)==0 ){
           addorderInfo(req.body);//Invoking add order Info
            console.log('orderInfo added successfully');
            return res.json({ message: 'orderInfo added successfully' });
    }
    else{
        res.send('order already exists');
        
    }
}
else{
    res.send('client does not exists');
}
        } catch (error) {
            console.error('Error adding order information to Redis:', error);
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        
            break;
        case 101: // Update order
        try{
            const orderInfo = req.body;
            const key = orderInfo.ClientID;
            const hashname = orderInfo.TenantId + "_" + orderInfo.OMSid;
            const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
             //Validating Client Id
             if(await redisclient.hexists(hashname,key)==1){
                //Validating order Id
                if(await redisclient.hexists(hashn,ke)==1 ){
            updateorderInfo(req.body);//Invoking update order Info
            console.log('OrderInfo updated successfully');
            return res.json({ message: 'OrderInfo updated successfully' });
            }
        else{
            res.send('order does not exist');
        }
    }
            else{
                res.send('Client does not exist');
            }
        }
        catch(error){
            console.error('Error updating order information to Redis:', error); 
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        break;
           
       
            case 102: // Remove order
    try {
        const orderInfo = req.body;
            const key = orderInfo.ClientID;
            const hashname = orderInfo.TenantId + "_" + orderInfo.OMSid;
            const ke = orderInfo.OrderID;
    const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
           //Validating Client Id
             if(await redisclient.hexists(hashname,key)==1){
                //Validating Order Id
                if(await redisclient.hexists(hashn,ke)==1 ){
            removeorderInfo(req.body);//Invoking removing order Info
            
        console.log('OrderInfo removed successfully from Redis');
        return res.json({ message: 'OrderInfo removed successfully' });
            }
            else{
                res.send('Order does not exist');
            }
        }
        else{
            res.send('Order does not exist'); 
        }
    } catch (error) {
        console.error('Error removing order information from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;

   
    case 103: // Get order
    try {
        const orderInfo = req.body;
        const key = orderInfo.ClientID;
        const hashname = orderInfo.TenantId + "_" + orderInfo.OMSid;
        const ke = orderInfo.OrderID;
const hashn = orderInfo.TenantId + "_" + orderInfo.OMSid + "_" + orderInfo.ClientID + "_" + orderInfo.Token;
//Validating Client Id
if(await redisclient.hexists(hashname,key)==1){
    //Validating Order Id
    if(await redisclient.hexists(hashn,ke)==1 ){
        const fetchedorderInfo = await getorderInfo(orderInfo);//Invoking getting order Info
        console.log('OrderInfo retrieved successfully from Redis');
        return res.json(fetchedorderInfo);
        }
    
        else{
            res.send('Order does not exist');  
        }
    }
   else{
    res.send('Order does not exist'); 
   }

    } catch (error) {
        console.error('Error getting order information from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;
    case 104: // Get All for Client order
    try {
        const { ClientID, TenantId, OMSid, Token } = req.body;
        const allOrderInfo = await getAllOrderInfoForClient(ClientID, TenantId, OMSid, Token);
        console.log('All order info for client retrieved successfully from Redis');
        return res.json(allOrderInfo);
    } catch (error) {
        console.error('Error getting all order information for client from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
    break;


        default:
            res.status(400).json({ error: 'Invalid OperationType' });
    }
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
