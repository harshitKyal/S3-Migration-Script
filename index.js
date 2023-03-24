const { MongoClient } = require('mongodb')
require('dotenv').config()
const path = require('path')
const aws = require('aws-sdk')
const fs = require('fs')

const agg = [{
        $project: {
            _id: 0,
            email: 1
        }
    },
    {
        $lookup: {
            from: 'FileHistory',
            let: {
                email: '$email'
            },
            pipeline: [{
                    $match: {
                        $expr: {
                            $eq: ['$email', '$$email']
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        email: 1,
                        filename: 1
                    }
                }
            ],
            as: 'result'
        }
    },
    {
        $match: {
            result: {
                $gt: [{
                        $size: '$result'
                    },
                    0
                ]
            }
        }
    },
    {
        $unwind: {
            path: '$result',
            preserveNullAndEmptyArrays: true
        }
    }
]

const MONGO_URL = process.env.MONGO_URI
const DB_NAME = 'myFirstDatabase'
const COLLECTION_NAME = 'Customer'
const PATH_OF_SANDBOX_FOLDER = path.join(__dirname + '/.sandbox')


const s3 = new aws.S3({
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
    signatureVersion: 'v4',
    region: 'ap-south-1'
})

async function UploadS3(key, body, userFolder) {
    const uploadParams = {
        Bucket: process.env.S3_BUCKET_NAME || '',
        // Add the required 'Key' parameter using the 'path' module.
        Key: userFolder + "/" + path.basename(key),
        // Add the required 'Body' parameter
        Body: body,
    };

    const uploadResponse = await s3.upload(uploadParams).promise()
    return uploadResponse
}

const uploadSandboxFiles = async(dbArray, sandboxFolderPath) => {
    for (i = 0; i < dbArray.length; i++) {
        const email = dbArray[i].email
        const fileName = dbArray[i].result.filename
        const filePath = path.join(sandboxFolderPath, fileName)
        console.log(filePath)
        if (fs.existsSync(filePath)) {
            await UploadS3(fileName, fs.createReadStream(filePath), email)

        }
    }
}

MongoClient.connect(MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
}).then(async client => {
    const coll = client.db(DB_NAME).collection(COLLECTION_NAME)
    const cursor = coll.aggregate(agg)
    const result = await cursor.toArray()
    await client.close()
    uploadSandboxFiles(result, PATH_OF_SANDBOX_FOLDER)
})
