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


async function getSpotifyNewMusic(accessToken) {
  var options = {
    url: `https://api.spotify.com/v1/browse/new-releases?limit=50`,
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    json: true
  };
  
  return new Promise((resolve, reject) => {
    request.get(options, function (error, response, body) {
      if (error) {
        resolve({error: true, message: "Something went wrong with the request. Try again.", statusCode: 500});
      }
      if (!error && response.statusCode === 401) {
        resolve({error: true, message: "Unauthorized", statusCode: 401});
      }
      if (!error && response.statusCode === 200) {
        resolve(body);
      }
    });
  });
}

async function getRelatedSpotifyArtists(accessToken, id) {
  var options = {
    url: `https://api.spotify.com/v1/artists/${id}/related-artists`,
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    json: true
  };
  
  return new Promise((resolve, reject) => {
    request.get(options, function (error, response, body) {
      if (error) {
        resolve({error: true, message: "Something went wrong with the request. Try again.", statusCode: 500});
      }
      if (!error && response.statusCode === 401) {
        resolve({error: true, message: "Unauthorized", statusCode: 401});
      }
      if (!error && response.statusCode === 200) {
        resolve(body);
      }
    });
  });
}

async function getSpotifyArtistTopTracks(accessToken, id) {
  var options = {
    url: `https://api.spotify.com/v1/artists/${id}/top-tracks?market=us`,
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    json: true
  };
  
  return new Promise((resolve, reject) => {
    request.get(options, function (error, response, body) {
      if (error) {
        resolve({error: true, message: "Something went wrong with the request. Try again.", statusCode: 500});
      }
      if (!error && response.statusCode === 401) {
        resolve({error: true, message: "Unauthorized", statusCode: 401});
      }
      if (!error && response.statusCode === 200) {
        resolve(body);
      }
    });
  });
}


async function getUserCollectiblesWithinLastSevenDays(pk, sevenDaysAgo) {
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and begins_with(sk, :sk)',
      FilterExpression: 'updated > :sevenDaysAgo',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Collectible|spotify|`,
          ':sevenDaysAgo': sevenDaysAgo
      },
  };
  
  const collectibles = await docClient.query(queryParams).promise();
  return collectibles;
}

async function getUserStreamedMillisecondsCollectiblesWithinLastSevenDays(pk, sevenDaysAgo) {
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and begins_with(sk, :sk)',
      FilterExpression: 'updated > :sevenDaysAgo',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Collectible|spotify|streamedMilliseconds`,
          ':sevenDaysAgo': sevenDaysAgo
      },
  };
  
  const collectibles = await docClient.query(queryParams).promise();
  return collectibles;
}

async function getUserAlbumsWithinLastSevenDays(pk, sevenDaysAgo) {
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and begins_with(sk, :sk)',
      FilterExpression: 'updated > :sevenDaysAgo',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Album|`,
          ':sevenDaysAgo': sevenDaysAgo
      },
  };
  
  const albums = await docClient.query(queryParams).promise();
  return albums;
}

async function getUserTracksWithinLastSevenDays(pk, sevenDaysAgo) {
  const queryParams = {
      TableName: 'radia-users',
      KeyConditionExpression: 'pk = :pk and begins_with(sk, :sk)',
      FilterExpression: 'updated > :sevenDaysAgo',
      ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': `Track|`,
          ':sevenDaysAgo': sevenDaysAgo
      },
  };
  
  const tracks = await docClient.query(queryParams).promise();
  return tracks;
}

const calculateProgress = (collectible) => {
  if (collectible.streamedMilliseconds <= 3600000 ) {
    if (parseInt(((collectible.streamedMilliseconds / 3600000) * 100).toFixed(1)) >= 1)
      return ((collectible.streamedMilliseconds / 3600000) * 100).toFixed(0);
    else
      return ((collectible.streamedMilliseconds / 3600000) * 100).toFixed(1);
  }

  if (collectible.streamedMilliseconds >= 3600000 && collectible.streamedMilliseconds < 3600000 * 5) {
    return ((collectible.streamedMilliseconds / (3600000 * 5)) * 100).toFixed(0);
  }  
  
  if (collectible.streamedMilliseconds >= 3600000 * 5 && collectible.streamedMilliseconds < 3600000 * 10) {
    return ((collectible.streamedMilliseconds / (3600000 * 10)) * 100).toFixed(0);
  }       

  if (collectible.streamedMilliseconds >= 3600000 * 10 && collectible.streamedMilliseconds < 3600000 * 15) {
    return ((collectible.streamedMilliseconds / (3600000 * 15)) * 100).toFixed(0);
  }        

  if (collectible.streamedMilliseconds >= 3600000 * 15 && collectible.streamedMilliseconds < 3600000 * 25) {
    return ((collectible.streamedMilliseconds / (3600000 * 25)) * 100).toFixed(0);
  }  
};

const msToTime = (duration) => {
  var milliseconds = parseInt((duration % 1000) / 100),
    seconds = Math.floor((duration / 1000) % 60),
    minutes = Math.floor((duration / (1000 * 60)) % 60),
    hours = Math.floor((duration / (1000 * 60 * 60)) % 24);

  hours = (hours < 10) ? "0" + hours : hours;
  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;
  
  if (hours && hours !== '00')
    return `${hours} hours ${minutes} minutes`;
    
  if (minutes && minutes !== '00')
    return `${minutes} minutes`;  
    
  if (seconds && seconds !== '00')
    return `${seconds} seconds`;        
};

const calculateTimeLeft = (collectible) => {
  if (collectible.streamedMilliseconds <= 3600000 ) {
    const timeLeft = msToTime(3600000 - collectible.streamedMilliseconds);
    return {timeLeft, name: `${collectible.artist.name} - 1 Hour Listening`};
  }

  if (collectible.streamedMilliseconds >= 3600000 && collectible.streamedMilliseconds < 3600000 * 5) {
    const timeLeft = msToTime(3600000 * 5 - collectible.streamedMilliseconds);
    return {timeLeft, name: `${collectible.artist.name} - 5 Hours Listening`};
  }  
  
  if (collectible.streamedMilliseconds >= 3600000 * 5 && collectible.streamedMilliseconds < 3600000 * 10) {
    const timeLeft = msToTime(3600000 * 10 - collectible.streamedMilliseconds);
    return {timeLeft, name: `${collectible.artist.name} - 10 Hours Listening`};
  }       

  if (collectible.streamedMilliseconds >= 3600000 * 10 && collectible.streamedMilliseconds < 3600000 * 15) {
    const timeLeft = msToTime(3600000 * 15 - collectible.streamedMilliseconds);
    return {timeLeft, name: `${collectible.artist.name} - 15 Hours Listening`};
  }        

  if (collectible.streamedMilliseconds >= 3600000 * 15 && collectible.streamedMilliseconds < 3600000 * 25) {
    const timeLeft = msToTime(3600000 * 25 - collectible.streamedMilliseconds);
    return {timeLeft, name: `${collectible.artist.name} - 25 Hours Listening`};
  }  
};

const sendEmail = (radiaSecrets, emailAddress, data) => {
  return new Promise((resolve, reject) => {
    var options = {
      url: "https://qk8wia3761.execute-api.us-east-1.amazonaws.com/prod/email/send",
      headers: {
        'x-api-key': radiaSecrets.RADIA_SERVER_API_KEY,
      },
      json: {
        "templateName": "Weekly Progress Email",
        "subject": "Your Weekly Progress 📊",
        "templateContent": [
          {"name": "artist_count", "content": data.numberOfArtists},
          {"name": "album_count", "content": data.numberOfAlbums},
          {"name": "track_count", "content": data.numberOfTracks},
          {"name": "collectible_count", "content": data.numberOfCompletedCollectibles},
          {"name": "hours_away_01", "content": data.closeToEarning[0].timeLeft},
          {"name": "hours_away_02", "content": data.closeToEarning[1].timeLeft},
          {"name": "hours_away_03", "content": data.closeToEarning[2].timeLeft},
          {"name": "nft_name_01", "content": data.closeToEarning[0].name},
          {"name": "nft_name_02", "content": data.closeToEarning[1].name},
          {"name": "nft_name_03", "content": data.closeToEarning[2].name},
          {"name": "top_pick_img_01", "content": `<a href="https://beta.radia.world/artist/${data.topPicks[0].artists[0].id}" target="_blank" rel="noopener noreferrer"><img src="${data.topPicks[0].images[0].url}" style="border-radius: 8px; border: 0px; width: 156px; height: 156px; margin: 0px;"/></a>`},
          {"name": "top_pick_img_02", "content": `<a href="https://beta.radia.world/artist/${data.topPicks[1].artists[0].id}" target="_blank" rel="noopener noreferrer"><img src="${data.topPicks[1].images[0].url}" style="border-radius: 8px; border: 0px; width: 156px; height: 156px; margin: 0px;"/></a>`},
          {"name": "top_pick_img_03", "content": `<a href="https://beta.radia.world/artist/${data.topPicks[2].artists[0].id}" target="_blank" rel="noopener noreferrer"><img src="${data.topPicks[2].images[0].url}" style="border-radius: 8px; border: 0px; width: 156px; height: 156px; margin: 0px;"/></a>`},
          {"name": "top_pick_text_01", "content": `<span style="font-family:'Urbanist'"><span style="font-weight:bold;">${data.topPicks[0].artists[0].name}</span> <br/> ${data.topPicks[0].name}</span>`},
          {"name": "top_pick_text_02", "content": `<span style="font-family:'Urbanist'"><span style="font-weight:bold;">${data.topPicks[1].artists[0].name}</span> <br/> ${data.topPicks[1].name}</span>`},
          {"name": "top_pick_text_03", "content": `<span style="font-family:'Urbanist'"><span style="font-weight:bold;">${data.topPicks[2].artists[0].name}</span> <br/> ${data.topPicks[2].name}</span>`}
        ],
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
    console.log("user ID", partitionKey);
    const refreshToken = event.refresh_token;
    const accessToken = await refreshSpotifyAccessToken(spotifySecrets, refreshToken);    
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).valueOf();

    // Get streamedMilliseconds user Collectibles within the last 7 days 
    // Filter by collectibles that have been updated
    const streamedMillisecondsCollectibles = await getUserStreamedMillisecondsCollectiblesWithinLastSevenDays(partitionKey, sevenDaysAgo);
    const filteredCollectibles = streamedMillisecondsCollectibles.Items.filter(collectible => collectible.updated > collectible.created);
    const numberOfArtists = filteredCollectibles.length;
    
    // Get Albums updated within the last 7 days 
    const albums = await getUserAlbumsWithinLastSevenDays(partitionKey, sevenDaysAgo);
    const numberOfAlbums = albums.Count;
    
    // Get Tracks updated within the last 7 days 
    const tracks = await getUserTracksWithinLastSevenDays(partitionKey, sevenDaysAgo);
    const numberOfTracks = tracks.Count;    
    
    // Get earned collectibles within the last 7 days
    const allCollectibles = await getUserCollectiblesWithinLastSevenDays(partitionKey, sevenDaysAgo);
    const completedCollectibles = allCollectibles.Items.filter(collectible => collectible.status === 'readyToMint' || "transaction" in collectible);
    const numberOfCompletedCollectibles = completedCollectibles.length;    
    
    // Get top three milliseconds streamed closest to earning collectibles.
    const closeToEarning = [];
    const inProgressFilteredCollectibles = allCollectibles.Items.filter(collectible =>  !("status" in collectible));
    const sortedInProgressCollectibles = inProgressFilteredCollectibles.sort((a, b) => calculateProgress(b) - calculateProgress(a)).slice(0, 3);
    
    // Loop through collectibles, calculate time left until earning
    for (const collectible of sortedInProgressCollectibles) {
            
      const timeLeft = calculateTimeLeft(collectible);
      closeToEarning.push(timeLeft);   
      
      // Note: if we ever want to use top tracks related to the users taste as a top pick, then use these. 
      // const relatedArtists = await getRelatedSpotifyArtists(accessToken, collectible.artist.id);
      // const randomIndexArtists = Math.floor(Math.random()*relatedArtists.artists.length);
      // const artist = relatedArtists.artists[randomIndexArtists];
      // const topTracks = await getSpotifyArtistTopTracks(accessToken, artist.id); 
      // const randomIndexTracks = Math.floor(Math.random()*topTracks.tracks.length);
      // topPicks.push(topTracks.tracks[randomIndexTracks]);
      // await wait(500);
    }
    
    // Get 3 random new music picks
    const topPicks = [];
    const newMusic = await getSpotifyNewMusic(accessToken);
    for (const _ of [1,2,3]) {
      const randomIndex = Math.floor(Math.random()*newMusic.albums.items.length);
      const pick = newMusic.albums.items[randomIndex];
      topPicks.push(pick);
    }
      
    console.log("sesing email with thisdata", numberOfArtists, numberOfAlbums, numberOfTracks, numberOfCompletedCollectibles, closeToEarning, topPicks);   
    const data = {
      numberOfArtists, 
      numberOfAlbums, 
      numberOfTracks, 
      numberOfCompletedCollectibles, 
      closeToEarning, 
      topPicks
    };
    
    //TODO: turning off for spotify review
    
    const user = await getUser(partitionKey);
    try {
      console.log("Sending email to:", user.email, "Email opt:", user.emailOptIn);
      if (user.emailOptIn === undefined || user.emailOptIn === true)
        await sendEmail(radiaSecrets, user.email, data);
    } catch (e) {
      console.log(e);
    }       

    return {success: true};

  } catch (err) {
    return {error: err};
  }
};