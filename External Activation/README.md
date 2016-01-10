##Usage

1. In SQL Management Studio execute script **01. Schema.sql**. This will create a new database.
1. Compile and run the C# project. Make sure you change the connection string properties to use the database created in previous step.
1. Open **02. Initiator - receive replies.sql** in new windows of SSMS, switch to a text mode <kbd>Ctrl+T</kbd>
1. Run **02. Initiator - receive replies.sql**. You will see messages that there was no data received.
1. Open **03. Initiator - send.sql** in new windows of SSMS, switch to a text mode <kbd>Ctrl+T</kbd>
1. Arrange these SSMS 2 windows, and the executing C# project console window, side by side so that you can see the messages.  
1. Run **03. Initiator - send.sql**. You will see messages appearing in all 3 windows.
