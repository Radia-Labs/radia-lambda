const AWS = require('aws-sdk');

exports.handler = async (event, context) => {
  // query radia-users for a ingle spotify user
  // initiate DailCheckSpotifyUserRecentPlayed step function
   try {  
    if(event.Records[0].dynamodb.NewImage) {
        if (event.Records[0].dynamodb.NewImage.pk && event.Records[0].dynamodb.NewImage.sk && event.Records[0].dynamodb.NewImage.sk['S'] == 'Integration|spotify') {
            console.log("PK Record", event.Records[0].dynamodb.NewImage.pk['S']);
            const pk = event.Records[0].dynamodb.NewImage.pk['S'];
            if (pk) {
              try {
                const docClient = new AWS.DynamoDB.DocumentClient();
                const dbParams = { 
                  TableName: 'radia-users',
                  KeyConditionExpression: 'pk = :pk and sk = :sk',
                  ExpressionAttributeValues: {
                      ':pk': pk,
                      ':sk': 'Integration|spotify'
                  } 
                };
                
                const STEP_FUNCTION_ARN = 'arn:aws:states:us-east-1:722242270459:stateMachine:SingleItemCheckSpotifyUserRecentPlayedStateMachine';    
                const data = await docClient.query(dbParams).promise();
                console.log(data.Items);
            
                console.log("Running step function...");
                const params = {
                  stateMachineArn: STEP_FUNCTION_ARN,
                  input: JSON.stringify(data.Items)
                };
                const stepfunctions = new AWS.StepFunctions();
                const executeResult = await stepfunctions.startExecution(params).promise();
                console.log(executeResult);
                
                return { body: JSON.stringify(data) };
              } catch (err) {
                return { error: err };
              }                
            } else {
                return { success: true };
            }
        } else {
            return { success: true };
        }
    } else {
        return { success: true };
    }
  } catch (err) {
    console.log("Error running initiateSingleItemCheckIfSpotifyUserRecentlyPlayed", err)
     return { error: err };
  }
};
