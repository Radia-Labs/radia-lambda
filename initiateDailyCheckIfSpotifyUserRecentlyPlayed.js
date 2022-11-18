const AWS = require('aws-sdk');

exports.handler = async (event, context) => {
  // Query radia-users for spotify users 
  // initiate DailyCheckSpotifyUserRecentPlayed step function
  
  try {
    const docClient = new AWS.DynamoDB.DocumentClient();
    const dbParams = { 
      TableName: 'radia-users',
      IndexName: 'sk-index',
      KeyConditionExpression: 'sk = :sk',
      ExpressionAttributeValues: {':sk': 'Integration|spotify'} 
    };
    
    const STEP_FUNCTION_ARN = 'arn:aws:states:us-east-1:722242270459:stateMachine:DailyCheckSpotifyUserRecentPlayedStateMachine';    
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
};
