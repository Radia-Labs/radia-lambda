const request = require('request');
const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient();

async function getSecrets(secretName) {
  const region = "us-east-1";
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

async function checkIfTrackReleaseDateWithinLast24Hours(items) {
  let tracks = [];
  items.forEach(async item => {
    const releaseDate = new Date(item.track.album.release_date);
    const now = new Date();
    const diff = now - releaseDate;
    if (diff > 0 && diff < (24 * 3600 * 1000)) {
      console.log(`${item.track.name} listened to within 24 hours of release`);
      tracks.push(item);
    }
  });
  return tracks;
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


async function createUserCollectible(pk, achievement, artist, track, user, status) {
  const newItem = {
    TableName: 'radia-users',
    Item: {
      ['pk']: pk,
      ['sk']: `Collectible|spotify|${achievement}|${artist.id}`,
      created: Date.now(),
      updated: Date.now(),
      achievement,
      artist,
      track: {
        context: track.context,
        played_at: track.played_at,
        ...track.track
      },
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
    console.log("Recently Played Tracks...", recentlyPlayedTracks);
    console.log("user ID", partitionKey);
    const user = await getUser(partitionKey);
    const tracks = await checkIfTrackReleaseDateWithinLast24Hours(recentlyPlayedTracks);
    console.log("Tracks ready to claim...", tracks);
    // Create email data
    for (const track of tracks) {
      for (const artist of track.track.artists) {
        
        // Get Artist data from Spotify 
        const artistData = await getSpotifyArtist(accessToken, artist.id);
        
        // Create User and Spotify Artist
        await createUserArtist(partitionKey, artistData);
        await createSpotifyArtist(artistData);        
        
        // Wait so that we don't kill the db
        await wait(500);
      }
      
        // Create User Collectible: run outside second loop to only create one collectible
      
        // Get Artist data from Spotify 
        const artistData = await getSpotifyArtist(accessToken, track.track.artists[0].id);
            
        // Create User Collectible
        console.log("Creating User Collectible...", track, "for artist", artistData);
        const status = 'readyToMint';
        await createUserCollectible(partitionKey, "streamedTrackInFirst24Hours", artistData, track, user, status);
    }
    
    // Send email to user with ready to mint achievements 
    
    //TODO: turning off for spotify review
    
    try {
      if (tracks.length && (user.emailOptIn === undefined || user.emailOptIn === true)) {
        console.log("Sending email to:", user.email);
        await sendEmail(radiaSecrets, user.email);
      }
    } catch (e) {
      console.log(e);
    }       

    return {success: true};

  } catch (err) {
    return {error: err};
  }
};