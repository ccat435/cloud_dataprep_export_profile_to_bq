const {google} = require('googleapis');
const request = require('then-request');

exports.jobprofileGCS = async (req, res) => {

  var jobID = req.body.jobid;

// Replace this variable with your Dataprep API token     
  var DataprepToken = "";
  
  // ------------------ GET DATAPREP JOB RESULTS PROFILE OBJECT --------------------------------
  
  var profileURL = "https://api.clouddataprep.com/v4/jobGroups/"+jobID+"/profile";

  var res_profile = await request('GET', profileURL, {
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+DataprepToken
    },
  });

  var jsonprofile = res_profile.body;

 // ------------------ GET DATAPREP JOB DETAILS OBJECT --------------------------------
  
  var jobgroupURL = "https://api.clouddataprep.com/v4/jobGroups/"+jobID+"?embed=wrangledDataset";

  var res_job = await request('GET', jobgroupURL, {
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+DataprepToken
    },
  });

  var jobdetails = JSON.parse(res_job.getBody());
  var recipeID = jobdetails.wrangledDataset.id;

  // ------------------ GET DATAPREP RECIPE OBJECT --------------------------------

  var recipe_endpoint = "https://api.clouddataprep.com/v4/wrangledDatasets/"+recipeID;

  var res_recipe = await request('GET', recipe_endpoint, {
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+ DataprepToken
    },
  });
  
  var jsonrecipe = JSON.parse(res_recipe.getBody());
  const name = jsonrecipe.name

// ------------------- UPLOAD FILE TO GCS -------------------------------------

  const {Storage} = require('@google-cloud/storage');

  const storage = new Storage();
  var date = new Date().toISOString().replace('T', '_').substr(0, 19);
  
// Replace this parameter with your GCS filepath. Do not change the +name +date section  
  const filename = 'ccarreras@trifacta.com/profiles/'+name+'_'+date+'.json';

// Replace this parameter with your bucket name
  const myBucket = storage.bucket('dataprep-staging-30f02b4d-28c9-4f90-b6ae-6d3b448ddbe8');
  const file = myBucket.file(filename);
  
  file.save(jsonprofile, function(err) {
    if (!err) {
      console.log(name);
    } else {
      console.log("ERROR: ", err);
    }
  });

// -------------------- Run profilerRules to BQ output -----------

  var jobrun_endpoint = "https://api.clouddataprep.com/v4/jobGroups";
  var res_recipe = await request('POST', jobrun_endpoint, {
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+ DataprepToken
    },
    json: {
        'wrangledDataset': {'id': 1653555}, // Replace the id: field with your WrangledDataset ID.
        'runParameters': {
          'overrides': {
            'data': [{'key': 'original_output_name', 'value': name}]
          }
        }
    }
  }).done(function (err) {
    if (err) res.send(err)
  })
  res.status(200).send;
  }