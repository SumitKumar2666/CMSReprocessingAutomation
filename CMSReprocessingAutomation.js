process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
const { BlobServiceClient } = require("@azure/storage-blob");

const azure_storage = require('azure-storage');
const { getSystemErrorMap } = require("util");
//prod
let STORAGE_CONNECTION_STRING = 'HIDEDFORSECURITY';
//dev
//let STORAGE_CONNECTION_STRING = 'HIDEDFORSECURITY';
const queueName = 'queue-curation';

let blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
const queueService = azure_storage.createQueueService(STORAGE_CONNECTION_STRING);

//----------------------------------------------------------------------------------------
let date_ob = new Date();
var dates = [];
let month = ("0" + (date_ob.getMonth() + 1)).slice(-2); //current month
let year = date_ob.getFullYear(); //current year

//for daily working
if(date_ob.getDate() == 1){
    month = ("0" + (date_ob.getMonth())).slice(-2); //previous month
    if(month == 01||month == 03||month == 05||month == 07||month == 08||month == 10||month == 12||month ==00) 
        dates.push(31);
    else if(month == 02){
        if(year%4!=0) //not leap year
        dates.push(28);
        else          //leap year
        dates.push(29);
    }
    else
        dates.push(30);

    if(month == 00){
            month = 12;
            year = date_ob.getFullYear() - 1;
    }
}
else{
    dates.push(("0" + (date_ob.getDate() - 1)).slice(-2)); //for yesterday
}
    


//for daily except friday, saturday rejects
// if(date_ob.getDay() == 1){ //if today is monday
//     dates.push(("0" + (date_ob.getDate() - 3)).slice(-2)); //for friday
//     dates.push(("0" + (date_ob.getDate() - 2)).slice(-2)); //for saturday
//     dates.push(("0" + (date_ob.getDate() - 1)).slice(-2)); //for sunday
// }
// else if(date_ob.getDate() == 1){
//     month = ("0" + (date_ob.getMonth())).slice(-2); //previous month
//     if(month == 01||month == 03||month == 05||month == 07||month == 08||month == 10||month == 12||month ==00)
//         dates.push(31);
//     else if(month == 02){
//         if(year%4!=0) //not leap year
//         dates.push(28);
//         else          //leap year
//         dates.push(29);
//     }
//     else
//         dates.push(30);
//     if(month == 00){
//         month = 12;
//         year = date_ob.getFullYear() - 1;
//     }
// }
// else{
//     dates.push(("0" + (date_ob.getDate() - 1)).slice(-2)); //for yesterday
// }
//dates.push('27');
// dates.push('17');
// dates.push('18');
// dates.push('19');

//----------------------------------------------------------------------------------------
var folderNames = ['Duplication500Failure','DuplicationFHIR500Failure','ReferenceHandler500Failure','Retry500Failure'];
var sourceSystems = ['Athena','OakStreet','IMAT','MHK','SSDS','Facets'];

const containerName = 'cont-rejected'; // source container
const targetContainerName = 'cont-rejected';
const containerClient = blobServiceClient.getContainerClient(containerName); 
const targetContainerClient = blobServiceClient.getContainerClient(targetContainerName);

const maxCountToBeProccessed = 200000; //
let countTotalFailures = 0;
//----------------------------------------------------------------------------------------

async function main() {
    //console.time('TimeTaken');
    for await(const date of dates){
        console.log("Working on Failures of date: "+date+"-"+month+"-"+year);
        countTotalFailures = 0;
        for await(const folder of folderNames){

            console.log("\n"+'-'.repeat(process.stdout.columns));
            console.log("Inside "+folder+" folder");
            console.log('-'.repeat(process.stdout.columns));
            
            for await(const item of sourceSystems){
                let folderPath = folder+'/'+year+'/'+month+'/'+date+'/'+item+'/';
                let targetFolderPath = folder+'/Manually_Processed/'+year+'/'+month+'/'+date+'/'+item+'/';
                
                let ifExists = await failuresExist(folder, folderPath, item);
                
                if(ifExists==1)
                    await reprocessFailures(folder, folderPath,targetFolderPath,item, date);
            
            }

        }
        
        var fs = require('fs');
        fs.appendFile('Count_500Failures.txt',year+"/"+month+"/"+date + " " +' -->'+' Todays Total : '+ countTotalFailures + ' ----------------------------------------'+'\n', function (err) {
        if (err) throw err;
        console.log('countTotalFailures for today updated in record!');
        }); 
        

        
    }
    //console.timeEnd('TimeTaken');
}

async function failuresExist(folderName, folderPath, item_sourceSystems){ //to check if failures exists in sourcefolder
        let iter = containerClient.listBlobsFlat({ prefix: folderPath});
        let check = await iter.next();
        if(check.done){
        console.log("\nNo files found in "+folderName+" folder for "+item_sourceSystems);
        return 0;
        }
        else{
        console.log("\nReprocessing for "+item_sourceSystems+" in "+folderName);
        return 1;
        }
}

async function reprocessFailures(folderName, folderPath, targetFolderPath, item_sourceSystems, date){ //to reprocess the failures

    let postText = ',"sourceSystem":"'+item_sourceSystems+'"}'; // to be added after file content
    let msgCount = 0; //counting number of successfull msgs sent to queue

    let iter = containerClient.listBlobsFlat({ prefix: folderPath});

    try {
        let i = 0;
        for await(const item of iter) {
            i++;
            if(i<maxCountToBeProccessed) {

                var str = item.name; // folderPath + fileName
                const blobClient = containerClient.getBlobClient(item.name);
                
                const downloadBlockBlobResponse = await blobClient.download(); // downloading from source container
                const downloaded = (
                    await streamToBuffer(downloadBlockBlobResponse.readableStreamBody)
                ).toString(); //file content
        

                let blobName = str.substring(folderPath.length); //getting filename from str
                    console.log("File name : ",blobName); //displaying file name
                let preText = '{"fileName":"' + blobName +'","payload":'; //to be added before file content
                
                let queuemessage = preText + downloaded + postText; 
                let flag = 1;    
                
                try {      
                    await queueService.createMessage(queueName, Buffer.from(queuemessage).toString('base64'), function (error) {
                        if (!error) {
                            console.log("queue message inserted");
                            
                        }
                        else{
                            flag = 0;
                            console.log("error inserting queue msg for: "+blobName);
                            console.log("Error message: " +error);
                        }
                    });
                
                    
                
                } catch (error) {
                 console.log("error in try block: ", error);
                  
                } finally {
                    if(flag==1){
                    
                        let name = targetFolderPath + blobName; //target filePath
                        let blockBlobClient = targetContainerClient.getBlockBlobClient(name);
                        let data = downloaded;
                        let uploadBlobResponse = blockBlobClient.upload(data, data.length); //copying source files to target
                        response = await blobClient.deleteIfExists(); //deleting source files
                        msgCount++;
                    }  
                }

                 
                 
            } else { //if i<maxCounttobeProcessed
                console.log("Max counts reached");
                break;
                }
        }

        console.log("Number of messages sent to queue: ",msgCount);
        countTotalFailures += msgCount;
    } catch (error) {
        console.log("error", error);
        } 

    //maintaining record for no of failures processed in a txt file.
    if(msgCount>0){ 
    var fs = require('fs');
    fs.appendFile('Count_500Failures.txt',year+"/"+month+"/"+date + " " + folderName + " "+item_sourceSystems+' -->'+' Total Count : '+ msgCount + '\n', function (err) {
      if (err) throw err;
      console.log('Count updated in record!');
    }); } 
}



async function streamToBuffer(readableStream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        readableStream.on("data", (data) => {
            chunks.push(data instanceof Buffer ? data : Buffer.from(data));
        });
        readableStream.on("end", () => {
            resolve(Buffer.concat(chunks));
        });
        readableStream.on("error", reject);
    });
}

main();