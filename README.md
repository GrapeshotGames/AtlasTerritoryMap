# (Experimental) Atlas Territory Map
Used as an example for Atlas to read from Redis and generate territory map data and host it out for end clients. Also it can optionally also generate the same data for a multi-level zoom maps (Example of this coming soon).

## Go Dependencies:
* go get github.com/go-redis/redis
* go get github.com/llgcode/draw2d
* go get github.com/GrapeshotGames/goquadtree/quadtree
* go get github.com/aws/aws-sdk-go

## Setup
Setup the config.json to point at your redis database and a few other things like the following should be configured:
```
    "Host": "",			//Can be left blank but usually just ip of hosting machine in simple case
    "GridSize": 1400000.0,	//Should match your ServerGrid.json file
    "ServersX": 15,		//Size of grid layout aka 2x2, 4x4, or 15x15
    "ServersY": 15,		//Size of grid layout aka 2x2, 4x4, or 15x15
```
Note: The config.json stays relative to binary path along with `./www` folder.

After that all you have to do is just run the binary (AtlasTerritoryMap.exe) and you should start seeing output like:
```
2019/01/07 16:35:40 Listening on  :8881
2019/01/07 16:35:41 Getting markers for game image
2019/01/07 16:35:41 Generating game images
2019/01/07 16:35:41 Updating 0 URLs in Redis
2019/01/07 16:35:56 Getting markers for game image
2019/01/07 16:35:56 game CRCs matched so skipping generation
2019/01/07 16:36:11 Getting markers for game image
2019/01/07 16:36:11 game CRCs matched so skipping generation
```

## Information
For more information about Atlas please visit [playatlas.com](https://playatlas.com).