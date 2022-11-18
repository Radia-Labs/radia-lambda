const request = require('request');
const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient();

async function getSecrets(secretName) {
  const region = "us-east-1";
  // Create a Secrets Manager client
  const client = new AWS.SecretsManager({
      region: region
  });  
  
  try {
    console.log('Getting secrets');
    let secret;
    const data = await client.getSecretValue({ SecretId: secretName }).promise();
    if (data.SecretString) secret = data.SecretString;
    return secret ? JSON.parse(secret) : secret;
  } catch (err) {
    if (err.code === 'ResourceNotFoundException') {
      console.log(`The requested secret ${secretName} was not found`);
    } else if (err.code === 'InvalidRequestException') {
      console.log(`The request was invalid due to: ${err.message}`);
    } else if (err.code === 'InvalidParameterException') {
      console.log(`The request had invalid params: ${err.message}`);
    }
    
  }   

}

async function refreshSpotifyAccessToken(spotifySecrets, refreshToken) {

  var authOptions = {
    url: "https://accounts.spotify.com/api/token",
    headers: {
      Authorization: `Basic ${Buffer.from(
        `${spotifySecrets.SPOTIFY_CLIENT_ID}:${spotifySecrets.SPOTIFY_CLIENT_SECRET}`
      ).toString("base64")}`,
    },
    form: {
      grant_type: "refresh_token",
      refresh_token: refreshToken,
    },
    json: true,
  };
  return new Promise((resolve, reject) => {
    request.post(authOptions, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        var token = body.access_token;
        resolve(token);
      }
    });
  });
}

async function getSpotifyRecentPlayed(accessToken) {
  var options = {
    url: "https://api.spotify.com/v1/me/player/recently-played",
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    json: true,
    qs: {limit: 50}
  };

  return new Promise((resolve, reject) => {
    request.get(options, function (error, response, body) {
      if (error)
        resolve({error: true, message: "Something went wrong with the request. Try again.", statusCode: 500});
      if (!error && response.statusCode === 401)
        resolve({error: true, message: "Unauthorized", statusCode: 401});
      if (!error && response.statusCode === 200) {
        resolve(body.items);
      }
    });
  });
}

async function getSpotifyArtist(accessToken, id) {
  var options = {
    url: `https://api.spotify.com/v1/artists/${id}`,
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    json: true
  };

  return new Promise((resolve, reject) => {
    request.get(options, function (error, response, body) {
      if (error)
        resolve({error: true, message: "Something went wrong with the request. Try again.", statusCode: 500});
      if (!error && response.statusCode === 401)
        resolve({error: true, message: "Unauthorized", statusCode: 401});
      if (!error && response.statusCode === 200) {
        resolve(body);
      }
    });
  });
}

function filterRecentlyPlayedByLast24Hours(tracks) {
  var ts = Math.round(new Date().getTime() / 1000);
  var tsYesterday = ts - (24 * 3600);
  return tracks.filter(track => {
    const playedAt = Math.round(new Date(track.played_at).getTime() / 1000);
    if (playedAt > tsYesterday) {
      return track;
    }
  });
}

async function getUser (pk) {
  
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and sk = :sk',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Auth|${pk}`
      },
  };

  const user = await docClient.query(queryParams).promise();
  return user.Items[0];
}

async function getUserCollectible (pk, artist, achievement) {
  
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and sk = :sk',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Collectible|spotify|${achievement}|${artist.id}`,
      },
  };

  const collectible = await docClient.query(queryParams).promise();
  return collectible.Items[0];
}

 async function createUserCollectible(pk, artist, achievement, streamedMilliseconds, user, status) {
  const newItem = {
    TableName: 'radia-users',
    Item: {
      ['pk']: pk,
      ['sk']: `Collectible|spotify|${achievement}|${artist.id}`,
      created: Date.now(),
      updated: Date.now(),
      achievement,
      streamedMilliseconds,
      artist,
      user: {
        profileImage: user.profileImage,
        verifierId: user.verifierId,
        name: user.name,
        addresses: user.addresses
      },      
      status
    },
  };

  return docClient
    .put(newItem)
    .promise()
    .then((_) => {
      return Promise.resolve(newItem.Item);
    });
}

async function updateUserCollectible(pk, artist, achievement, data) {
  let updateExpression = '',
      attrExpression = {},
      values = {};
  for (let key in data) {
      updateExpression += updateExpression === '' ? `SET #${key} = :${key}` : `, #${key} = :${key}`;
      attrExpression = {
          ...attrExpression,
          [`#${key}`]: key
      };
      values = {
          ...values,
          [`:${key}`]: data[key]
      };
  }

  const updateParams = {
      TableName: 'radia-users',
      Key: {
        ['pk']: pk,
        ['sk']: `Collectible|spotify|${achievement}|${artist.id}`
      },
      ExpressionAttributeNames: attrExpression,
      UpdateExpression: updateExpression,
      ExpressionAttributeValues: values,
  };

  return docClient
    .update(updateParams)
    .promise();
}

 async function createUserArtist(pk, artist) {
  const newItem = {
    TableName: 'radia-users',
    Item: {
      ['pk']: pk,
      ['sk']: `Artist|spotify|${artist.id}`,
      created: Date.now(),
      updated: Date.now(),
      ...artist,
    },
  };

  return docClient
    .put(newItem)
    .promise()
    .then((_) => {
      return Promise.resolve(newItem.Item);
    });
}

 async function createUserAlbum(pk, album) {
  const newItem = {
    TableName: 'radia-users',
    Item: {
      ['pk']: pk,
      ['sk']: `Album|spotify|${album.id}`,
      created: Date.now(),
      updated: Date.now(),
      ...album
    },
  };

  return docClient
    .put(newItem)
    .promise()
    .then((_) => {
      return Promise.resolve(newItem.Item);
    });
}

 async function createUserTrack(pk, track) {
  const newItem = {
    TableName: 'radia-users',
    Item: {
      ['pk']: pk,
      ['sk']: `Track|spotify|${track.id}`,
      created: Date.now(),
      updated: Date.now(),
      ...track
    },
  };

  return docClient
    .put(newItem)
    .promise()
    .then((_) => {
      return Promise.resolve(newItem.Item);
    });
}

 async function createSpotifyArtist(artist) {
  const newItem = {
    TableName: 'radia-artists',
    Item: {
      ['pk']: `${artist.id}`,
      ['sk']: `Artist|spotify|${artist.id}`,
      created: Date.now(),
      updated: Date.now(),
      ...artist,
    },
  };

  return docClient
    .put(newItem)
    .promise()
    .then((_) => {
      return Promise.resolve(newItem.Item);
    });
}

function millisecondsGreaterThanDuration(ms, multiple) {
  const oneHour = 3600000;
  const duration = oneHour * multiple;
  if (ms >= duration) {
    return true;
  }
  return false;
}

async function checkAndWriteAchievement(user, partitionKey, artist, item, achievement) {
  
  // Create email data
  let emailData;  

  // Check User Collectible and increment millisecons
  const userCollectible = await getUserCollectible(partitionKey, artist, achievement) ;
  if (!userCollectible) {
    console.log("Creating User Collectible For Artist: ", artist, item.track.duration_ms);
    await createUserCollectible(partitionKey, artist, "streamedMilliseconds", item.track.duration_ms, user);
  } else {
    console.log("Found User Collectible", userCollectible);
    console.log("Incrementing streamedMilliseconds", userCollectible.streamedMilliseconds, item.track.duration_ms);
    const incrementedMilliseconds = userCollectible.streamedMilliseconds + item.track.duration_ms;
    const updatedData = {
      streamedMilliseconds: incrementedMilliseconds,
      updated: Date.now(),
    };
    await updateUserCollectible(partitionKey, artist, "streamedMilliseconds", updatedData);
    
    if (millisecondsGreaterThanDuration(incrementedMilliseconds, 1)) {
      console.log("1 Hour Achievement unlocked! Minting NFT and creating new User Collectible:");
      const streamed01HourUserCollectible = await getUserCollectible(partitionKey, artist, 'streamed01Hour') ;
      if (!streamed01HourUserCollectible) {
        console.log("Did not find streamed01Hour, minting NFT.");
        const status = "readyToMint";
        await createUserCollectible(partitionKey, artist, "streamed01Hour", incrementedMilliseconds, user, status);
        emailData = user; 
      }
    }

    if (millisecondsGreaterThanDuration(incrementedMilliseconds, 5)) {
      console.log("5 Hour Achievement unlocked! Minting NFT and creating new User Collectible:");
      const streamed05HourUserCollectible = await getUserCollectible(partitionKey, artist, 'streamed05Hours') ;
      if (!streamed05HourUserCollectible) {
        console.log("Did not find streamed05Hours, minting NFT.");
        const status = 'readyToMint';
        await createUserCollectible(partitionKey, artist, "streamed05Hours", incrementedMilliseconds, user, status);
        emailData = user; 
      }            
    }        

    if (millisecondsGreaterThanDuration(incrementedMilliseconds, 10)) {
      console.log("10 Hour Achievement unlocked! Minting NFT and creating new User Collectible:");
      const streamed10HourUserCollectible = await getUserCollectible(partitionKey, artist, 'streamed10Hours') ;
      if (!streamed10HourUserCollectible) {
        console.log("Did not find streamed10Hours, minting NFT.");
        const status = 'readyToMint';
        await createUserCollectible(partitionKey, artist, "streamed10Hours", incrementedMilliseconds, user, status);
        emailData = user; 
      }          
    }
    
    if (millisecondsGreaterThanDuration(incrementedMilliseconds, 15)) {
      console.log("15 Hour Achievement unlocked! Minting NFT and creating new User Collectible:");
      const streamed15HourUserCollectible = await getUserCollectible(partitionKey, artist, 'streamed15Hours') ;
      if (!streamed15HourUserCollectible) {
        console.log("Did not find streamed15Hours, minting NFT.");
        const status = 'readyToMint';
        await createUserCollectible(partitionKey, artist, "streamed15Hours", incrementedMilliseconds, user, status);
        emailData = user; 
      }                      
    }        

    if (millisecondsGreaterThanDuration(incrementedMilliseconds, 25)) {
      console.log("25 Hour Achievement unlocked! Minting NFT and creating new User Collectible:");
      const streamed25HourUserCollectible = await getUserCollectible(partitionKey, artist, 'streamed25Hours') ;
      if (!streamed25HourUserCollectible) {
        console.log("Did not find streamed25Hours, minting NFT.");
        const status = 'readyToMint';
        await createUserCollectible(partitionKey, artist, "streamed25Hours", incrementedMilliseconds, user, status);
        emailData = user; 
      }                   
    }        
  }  
  
  return emailData;
}

const sendEmail = (radiaSecrets, emailAddress) => {
  return new Promise((resolve, reject) => {
    var options = {
      url: "https://qk8wia3761.execute-api.us-east-1.amazonaws.com/prod/email/send",
      headers: {
        'x-api-key': radiaSecrets.RADIA_SERVER_API_KEY,
      },
      json: {
        "templateName": "You've Earned a Collectible",
        "subject": "You Earned a Collectible 🎉",
        "templateContent": [{}],
        "emailAddress": emailAddress,
      }
    };    
    request.post(options, function (error, response, body) {
      console.log(response, body);
      if (error) {
        console.log("Error posting to radia-server /email/send.", error);
        resolve({success: false, error});
      }
      if (!error && response.statusCode === 200) {
        resolve({success: true, transaction: body});
      }
    });
  });  
};

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

exports.handler = async (event, context) => {
    try {
        const spotifySecretName = "prod/Radia/Spotify";
        const spotifySecrets = await getSecrets(spotifySecretName); 
        const radiaSecretName = "prod/Radia/API";
        const radiaSecrets = await getSecrets(radiaSecretName);         
        const partitionKey = event.pk;
        const refreshToken = event.refresh_token;
        const accessToken = await refreshSpotifyAccessToken(spotifySecrets, refreshToken);
        const recentlyPlayedTracks = await getSpotifyRecentPlayed(accessToken);
        console.log("user ID", partitionKey);
        const user = await getUser(partitionKey);
        const tracksInLast24Hours = filterRecentlyPlayedByLast24Hours(recentlyPlayedTracks);
        console.log("Tracks played in the last 24 hours:", tracksInLast24Hours);
        
        let emailArray = [];
        
        // TODO: optimize this call. Use Dynamodb BactchWrite.
        for (const item of tracksInLast24Hours) {
          
          // Create Album in database
          await createUserAlbum(partitionKey, item.track.album);
          
          // Create Track in database
          await createUserTrack(partitionKey, item.track);
          
          for (const artist of item.track.artists) {
            
            // Get Artist data from Spotify 
            const artistData = await getSpotifyArtist(accessToken, artist.id);
            
            // Create Artists in databases
            await createUserArtist(partitionKey, artistData);
            await createSpotifyArtist(artistData);
            
            // Check achievements and mint NFT if milliseconds eclipsed 
            // Append data to email array
            const emailData = await checkAndWriteAchievement(user, partitionKey, artistData, item, 'streamedMilliseconds');
            if (emailData)
              emailArray.push(emailData);
              
            // Wait so that we don't kill the db
            await wait(500);

          }
        }
        
        // Send email to user with  ready to mint achievements 
        
        //TODO: turning off for spotify 
        // try {
        //   console.log("Email Array Data:", emailArray);
        //   if (emailArray.length && (emailArray[0].emailOptIn === undefined || emailArray[0].emailOptIn === true)) {
        //     console.log("Sending email to:", emailArray[0].email);
        //     await sendEmail(radiaSecrets, emailArray[0].email);
        //   }
        // } catch (e) {
        //   console.log(e);
        // }      


        return {success: true};
    }
    catch(err) {
        console.log("Error running handler...", err);
        return {success: false};
    }
    
};