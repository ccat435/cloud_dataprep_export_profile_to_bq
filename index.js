const {google} = require('googleapis');
const request = require('then-request');

exports.jobprofileGCS = async (req, res) => {

  var jobID = req.body.jobid;
     
  var DataprepToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbklkIjoiYjYzNTljMjMtOGMxNy00YTJhLTljNzQtNWJlOWU5ODBmMjA3IiwiaWF0IjoxNTk5MjUxMDEwLCJhdWQiOiJ0cmlmYWN0YSIsImlzcyI6ImRhdGFwcmVwLWFwaS1hY2Nlc3MtdG9rZW5AdHJpZmFjdGEtZ2Nsb3VkLXByb2QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJzdWIiOiJkYXRhcHJlcC1hcGktYWNjZXNzLXRva2VuQHRyaWZhY3RhLWdjbG91ZC1wcm9kLmlhbS5nc2VydmljZWFjY291bnQuY29tIn0.l3ieSWS22wcjZ-SjF8avJD1mGJ1rnZFfdZCHsc76_w-G2HlbJlyzP0z6IkAVLlGXG9lJ8112SQLyaqDiXZpb4XRlSpoTdsIA1SLLWEb9wms5JBYyTCTEjO0GmapgeE_siMOYysU17GGNkYF1D8-_BM23e_nvQVpIdq8jFez9nsw6k6gBXnj5WnCBVGSvLdJsCEwaZFEu400QgLvc0cUiMu7_8AvyG4pd5vMp2YUxuaZgjJBiU54a_MP84iTeSdtNb1z-ZP4JZLXZ6w59AOoUQ7tT2vnR2vVAISAEIS1OMj2Ce7cdvrX5_-pTe0Zr_Cc_XWYmOFGxzY885sb2rVkGDA";
  
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
  const filename = 'ccarreras@trifacta.com/profiles/'+name+'_'+date+'.json';

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
        'wrangledDataset': {'id': 1653555},
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