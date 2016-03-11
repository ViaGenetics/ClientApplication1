# Genesis Client Application
##Introduction
Genesis client appliication allows you to transfer securely your genomic data over to our cloud based platform. 
This application runs as a service in the background and transfers the data files (FASTQ or VCF) you specify using 
the Data Upload module on Genesis.

##How does it work
After a user creates an Upload request The Genesis Project administrators will review and approve your request in order to kick off the trasnfer automatically.
Users are going to be able to track each sample progress via web application.

These are the steps involved in the process:
- Create an Upload Request
- Deploy client app, if needed - this is a one time step. Once a client app was deployed can be used to handle future data transfers if the data is available form the same server 
- Provide Clinical information
- Provide Genomic information
- Review and submit your order

You will recieve and email once your request has being approved.

##How to deploy

For instructions go to our Wiki page  [Deployment instructions](https://github.com/ViaGenetics/GenesisClientApp/wiki/Deployment-instructions) 

Note: The configuation file points to staging environment. Change to Production once it is tested and ready to go live.


##How to operate

For available commands go to our Wiki page [Available commnads](https://github.com/ViaGenetics/GenesisClientApp/wiki/Available-commands) 



##Support

For support email support@viagenetics.com, set Subject to "Genesis Client App"




