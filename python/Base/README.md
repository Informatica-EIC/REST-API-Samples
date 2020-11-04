# Base Classes

These base classes define a Connection class and a Basic InformaticaAPI class.

### Connection Class
The Connection class provides functionality to set-up a connection to whatever URL you want.

### InformaticaAPI Class
The InformaticaAPI class provides functionality for using basic EDC APIs. The scope of this class should be limited to defining basic APIRequests.

### EDCTools Class
The EDCTools class extends the InformaticaAPI class to utilize those API calls to perform a variety of tasks.

### TODO
The idea behind these two classes is to provide basic functionality for accessing EDC that is easy to read and use. With the extensibility of it, we can easily add Plug-ins that extend the InformaticaAPI class to perform specific actions. Example of this can be seen in the EDCTools class.
1. Add more basic API requests to InformaticaAPI
2. Add more functionality to EDCTools

### Credentials
There are two ways to provide your credentials:
1. Provide your credentials in `credentials.py`.
2. Input them in the terminal when prompted.

### Examples
To use the InformaticaAPI class create an instance of the object. Examples can be seen below.

```
from InformaticaAPI import InformaticaAPI
# Credentials will be either read from credentials.py or prompted in the terminal
informaticaAPIObj = InformaticaAPI()
# Example calls
informaticaAPIObj.getObject("<objectID>")
informaticaAPIObj.getAssociations()
informaticaAPIObj.getRelationships(["<objectID>"], ["<associationID>"])
informaticaAPIObj.search("Query")
```

Examples on how to use the EDCTools class, look at `examples/examples.py`.


Examples are to come.