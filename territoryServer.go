package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GrapeshotGames/goquadtree/quadtree"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-redis/redis"
	"github.com/llgcode/draw2d/draw2dimg"
)

// Marker represents a territory flag roughly as stored in redis
type Marker struct {
	serverX        int // from redis key which is serverId
	serverY        int // from redis key which is serverId
	tribeOrOwnerID uint64
	relX           float64 // uint16 in redis
	relY           float64 // uint16 in redis
	markerType     uint8
	UnusedExtra1   uint8
	UnusedExtra2   uint8
	UnusedExtra    uint8
}

// EntityInfo represents Marker / Entity relationship
type EntityInfo struct {
	entityID uint64
	parentID uint64
	marker   Marker
}

// ClaimFlagOutputEntry for saving compressed file out
type ClaimFlagOutputEntry struct {
	X, Y uint16
}

// FlagOwnerOutputHeader for saving compressed file out
type FlagOwnerOutputHeader struct {
	TribeOrPlayerID uint64
	LandClaims      []ClaimFlagOutputEntry
	WaterClaims     []ClaimFlagOutputEntry
	//ServerIdx uint16 (10 bits)
	//ExtraFlags? (4 bits)
}

// ByTribeOrPlayerID implements sort.Interface for []FlagOwnerOutputHeader based on the TribeOrPlayerID field.
type ByTribeOrPlayerID []FlagOwnerOutputHeader

func (a ByTribeOrPlayerID) Len() int           { return len(a) }
func (a ByTribeOrPlayerID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTribeOrPlayerID) Less(i, j int) bool { return a[i].TribeOrPlayerID < a[j].TribeOrPlayerID }

// VirtualBounds represents marker in virtual coordinates for quad tree
type VirtualBounds struct {
	x      float64
	y      float64
	radius float64
	marker Marker
}

// BoundingBox for quadtree.BoundingBoxer interface
func (v VirtualBounds) BoundingBox() quadtree.BoundingBox {
	return quadtree.BoundingBox{
		MinX: v.x - v.radius,
		MaxX: v.x + v.radius,
		MinY: v.y - v.radius,
		MaxY: v.y + v.radius,
	}
}

// RedisConfiguration holds Atlas database configuration
type RedisConfiguration struct {
	Name     string
	URL      string
	Port     int
	Password string
}

// Configuration holds applicaiton configuration
type Configuration struct {
	EnableTileGeneration bool                 // Turn on/off generation for web page
	EnableGameGeneration bool                 // Turn on/off generation for game
	Host                 string               // Host adapter for http listen
	Port                 uint16               // Port for http listen
	AlternativeURL       string               // Alternative URL (e.g. S3) for game and web viewer
	WWWDir               string               // Directory holding generated images
	FetchRateInSeconds   int                  // Polling rate
	DatabaseConnections  []RedisConfiguration // Databases config
	ServersX             int                  // Number of servers in X dim
	ServersY             int                  // Number of servers in Y dim
	GameSize             int                  // Number of pixels for in-game images
	TileSize             int                  // Number of pixels per tile
	MaxZoom              uint                 // Maxium zoom level
	GridSize             float64              // UE Coordinate range per server
	LandRadiusUE         float64              // UE radius of land marker
	WaterRadiusUE        float64              // UE radius of water marker
	CircleAlpha          uint8                // Alpha value for circles 0-100%
	AtlasS3URL           string               // Alternative S3 URL for something like Minio
	AtlasS3Region        string               // AWS lib needs a region, no default?
	AtlasS3AccessID      string               // AWS access id, if empty disables S3 upload
	AtlasS3SecretKey     string               // AWS Secret key
	AtlasS3BucketName    string               // AWS S3 bucket name
	AtlasS3KeyPrefix     string               // AWS SE key prefix
}

func (c *Configuration) getDatabaseByName(name string) RedisConfiguration {
	for _, v := range c.DatabaseConnections {
		if v.Name == name {
			return v
		}
	}
	return RedisConfiguration{Name: "not found", URL: "localhost", Port: 6379, Password: ""}
}

var config Configuration
var colors = [...]string{
	//"red",
	//"green",
	"yellow",
	"blue",
	//"orange",
	"purple",
	//"cyan",
	//"magenta",
	//"lime",
	//"pink",
	//"teal",
	//"lavender",
	//"brown",
	//"beige",
	//"maroon",
	//"olive",
	"coral",
	//"navy",
}
var colorValues = map[string]color.NRGBA{
	"black": color.NRGBA{0x00, 0x00, 0x00, 0xff},
	"gray":  color.NRGBA{0xa9, 0xa9, 0xa9, 0xff},
	//"red":      color.NRGBA{0xff, 0x00, 0x00, 0xff},
	//"green":    color.NRGBA{0x00, 0x80, 0x00, 0xff},
	"yellow": color.NRGBA{0xff, 0xff, 0x00, 0xff},
	"blue":   color.NRGBA{0x00, 0x00, 0xff, 0xff},
	//"orange": color.NRGBA{0xff, 0xa5, 0x00, 0xff},
	"purple": color.NRGBA{0x80, 0x00, 0x80, 0xff},
	//"cyan":   color.NRGBA{0x00, 0xff, 0xff, 0xff},
	// "magenta":  color.NRGBA{0xff, 0x00, 0xff, 0xff},
	// "lime":     color.NRGBA{0x00, 0xff, 0x00, 0xff},
	// "pink":     color.NRGBA{0xff, 0xc0, 0xcb, 0xff},
	//"teal":     color.NRGBA{0x00, 0x80, 0x80, 0xff},
	// "lavender": color.NRGBA{0xe6, 0xe6, 0xfa, 0xff},
	// "brown":    color.NRGBA{0xa5, 0x2a, 0x2a, 0xff},
	// "beige":    color.NRGBA{0xf5, 0xf5, 0xdc, 0xff},
	// "maroon":   color.NRGBA{0x80, 0x00, 0x00, 0xff},
	// "olive":    color.NRGBA{0x80, 0x80, 0x00, 0xff},
	"coral": color.NRGBA{0xff, 0x7f, 0x50, 0xff},
	// "navy":     color.NRGBA{0x00, 0x00, 0x80, 0xff},
}

func loadConfig(path string) (cfg Configuration, err error) {
	var file *os.File

	file, err = os.Open(path)
	defer file.Close()
	if err != nil {
		return
	}

	decoder := json.NewDecoder(file)

	cfg = Configuration{
		EnableTileGeneration: false,
		EnableGameGeneration: true,
		Host:                 "",
		Port:                 8881,
		AlternativeURL:       "",
		WWWDir:               "./www",
		FetchRateInSeconds:   15,
		DatabaseConnections: []RedisConfiguration{
			{
				Name:     "Default",
				URL:      "localhost",
				Port:     6379,
				Password: "foobared",
			},
			{
				Name:     "TribeDB",
				URL:      "localhost",
				Port:     6379,
				Password: "foobared",
			},
		},
		ServersX:          3,
		ServersY:          3,
		GameSize:          2048,
		TileSize:          256,
		MaxZoom:           7,
		GridSize:          1400000,
		LandRadiusUE:      10000,
		WaterRadiusUE:     21000,
		CircleAlpha:       128,
		AtlasS3URL:        "",
		AtlasS3Region:     "us-east-1",
		AtlasS3AccessID:   "",
		AtlasS3SecretKey:  "",
		AtlasS3BucketName: "",
		AtlasS3KeyPrefix:  "",
	}

	if err = decoder.Decode(&cfg); err != nil {
		return
	}

	if len(cfg.AtlasS3KeyPrefix) > 0 && !strings.HasSuffix(cfg.AtlasS3KeyPrefix, "/") {
		cfg.AtlasS3KeyPrefix += "/"
	}

	return
}

// parseServerID unpacks the packed server ID. Each Server has an X and Y ID which
// corresponds to its 2D location in the game world. The ID is packed into
// 32-bits as follows:
//   +--------------+--------------+
//   | X (uint16_t) | Y (uint16_t) |
//   +--------------+--------------+
func parseServerID(packed string) (split [2]uint16, err error) {
	var id uint64
	id, err = strconv.ParseUint(packed, 10, 32)
	if err != nil {
		return
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(id))
	split[0] = binary.LittleEndian.Uint16(buf[2:]) // X
	split[1] = binary.LittleEndian.Uint16(buf[:2]) // Y
	return
}

func isTribeID(tribeID uint64) bool {
	return tribeID > 1000000000+50000
}

// getTribeColor returns a consistent color for a given tribe id
func getTribeColor(tribeID uint64) color.NRGBA {
	if tribeID == 0 {
		return colorValues["black"]
	}
	if !isTribeID(tribeID) {
		return colorValues["gray"]
	}
	idx := int(tribeID % uint64(len(colors)))
	color := colors[idx]
	return colorValues[color]
}

// MapOptions holds map construction information
type MapOptions struct {
	filename      string
	actualPixels  int
	virtualPixels int
	virtualClip   image.Rectangle
}

func createQuadTree(opts *MapOptions, markers []Marker) *quadtree.QuadTree {
	var virtualPixelsPerServer float64
	if config.ServersX >= config.ServersY {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersX)
	} else {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersY)
	}
	virtualWaterRadius := virtualPixelsPerServer * config.WaterRadiusUE / config.GridSize

	bb := quadtree.BoundingBox{MinX: 0, MinY: 0, MaxX: float64(opts.virtualPixels), MaxY: float64(opts.virtualPixels)}
	qt := quadtree.NewQuadTree(bb)

	for _, marker := range markers {
		vServerOffsetX := float64(marker.serverX) * virtualPixelsPerServer
		vServerOffsetY := float64(marker.serverY) * virtualPixelsPerServer
		vX := (float64(marker.relX) * virtualPixelsPerServer) + vServerOffsetX
		vY := (float64(marker.relY) * virtualPixelsPerServer) + vServerOffsetY
		v := VirtualBounds{
			x:      vX,
			y:      vY,
			radius: virtualWaterRadius,
			marker: marker,
		}
		qt.Add(v)
	}
	return &qt
}

func tempFileName(prefix, suffix string) string {
	return fmt.Sprintf("%s%x%s", prefix, rand.Int31(), suffix)
}

func uploadToS3(file string) error {
	// Punt if no S3 config info
	if len(config.AtlasS3AccessID) == 0 {
		return nil
	}

	// Open input file
	in, err := os.Open(file)
	if err != nil {
		return err
	}
	defer in.Close()

	// Prep S3 connection
	session, err := session.NewSession(&aws.Config{
		Region:      &config.AtlasS3Region,
		Credentials: credentials.NewStaticCredentials(config.AtlasS3AccessID, config.AtlasS3SecretKey, ""),
	})
	if err != nil {
		return err
	}
	svc := s3.New(session)
	uploader := s3manager.NewUploaderWithClient(svc)

	// Upload the file
	key := config.AtlasS3KeyPrefix + strings.TrimPrefix(file, path.Clean(config.WWWDir)+"/")
	upParams := &s3manager.UploadInput{
		Bucket: &config.AtlasS3BucketName,
		Key:    &key,
		Body:   in,
	}
	_, err = uploader.Upload(upParams)
	return err
}

func generateImage(opts *MapOptions, quadTree *quadtree.QuadTree) {
	var virtualPixelsPerServer float64
	if config.ServersX >= config.ServersY {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersX)
	} else {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersY)
	}
	virtualLandRadius := virtualPixelsPerServer * config.LandRadiusUE / config.GridSize
	virtualWaterRadius := virtualPixelsPerServer * config.WaterRadiusUE / config.GridSize
	virtualToActual := float64(opts.actualPixels) / float64(opts.virtualClip.Max.X-opts.virtualClip.Min.X+1)

	maskSrcImg := image.NewRGBA(image.Rect(0, 0, opts.actualPixels, opts.actualPixels))
	gc := draw2dimg.NewGraphicContext(maskSrcImg)

	qtBB := quadtree.BoundingBox{
		MinX: float64(opts.virtualClip.Min.X),
		MaxX: float64(opts.virtualClip.Max.X),
		MinY: float64(opts.virtualClip.Min.Y),
		MaxY: float64(opts.virtualClip.Max.Y),
	}
	for _, iVB := range quadTree.Query(qtBB) {
		vb := iVB.(VirtualBounds)

		// marker adjusted for clip zone
		tX := vb.x - float64(opts.virtualClip.Min.X)
		tY := vb.y - float64(opts.virtualClip.Min.Y)

		// filter points outside of clip + gutter
		if tX < -virtualWaterRadius || tY < -virtualWaterRadius || tX >= float64(opts.virtualClip.Max.X)+virtualWaterRadius || tY >= float64(opts.virtualClip.Max.Y)+virtualWaterRadius {
			continue
		}

		// marker in image coordinates
		iX := tX * virtualToActual
		iY := tY * virtualToActual

		// radius in image coordinates
		iRadius := 1.0
		switch vb.marker.markerType {
		case 0:
			iRadius = virtualLandRadius * virtualToActual
		case 1:
			iRadius = virtualWaterRadius * virtualToActual
		}
		if iRadius < 1 {
			iRadius = 1.0
		}

		// render marker
		color := getTribeColor(vb.marker.tribeOrOwnerID)
		gc.SetStrokeColor(color)
		gc.SetFillColor(color)
		gc.ArcTo(iX, iY, iRadius, iRadius, 0.0, 2*math.Pi)
		gc.Fill()
	}

	// Generate transparent final image using the opaque maskSrcImg
	finalImg := image.NewRGBA(image.Rect(0, 0, opts.actualPixels, opts.actualPixels))
	draw.DrawMask(finalImg, finalImg.Bounds(), maskSrcImg, image.ZP, image.NewUniform(color.Alpha{config.CircleAlpha}), image.ZP, draw.Over)

	// save the a tmp file
	dir := path.Dir(opts.filename)
	os.MkdirAll(path.Dir(opts.filename), os.ModePerm)
	tmpFilename := path.Join(dir, tempFileName("tmp_", ".png"))
	f, _ := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	png.Encode(f, finalImg)
	f.Close()

	// delete old file and rename tmp
	os.Remove(opts.filename)
	os.Rename(tmpFilename, opts.filename)

	uploadToS3(opts.filename)
}

type claimCircle struct {
	location image.Point
	id       int64
}

func generateCompressedFile(opts *MapOptions, quadTree *quadtree.QuadTree) {
	// Setup
	var virtualPixelsPerServer float64
	if config.ServersX >= config.ServersY {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersX)
	} else {
		virtualPixelsPerServer = float64(opts.virtualPixels / config.ServersY)
	}
	//	virtualLandRadius := virtualPixelsPerServer * config.LandRadiusUE / config.GridSize
	virtualWaterRadius := virtualPixelsPerServer * config.WaterRadiusUE / config.GridSize
	virtualToActual := float64(opts.actualPixels) / float64(opts.virtualClip.Max.X-opts.virtualClip.Min.X+1)

	//TODO: Cleanup and remote the whole per server option on this one
	SrcPixels := uint16(opts.actualPixels)
	IDMap := make(map[uint64]FlagOwnerOutputHeader)

	//Draw territories
	qtBB := quadtree.BoundingBox{
		MinX: float64(opts.virtualClip.Min.X),
		MaxX: float64(opts.virtualClip.Max.X),
		MinY: float64(opts.virtualClip.Min.Y),
		MaxY: float64(opts.virtualClip.Max.Y),
	}
	for _, iVB := range quadTree.Query(qtBB) {
		vb := iVB.(VirtualBounds)

		// marker adjusted for clip zone
		tX := vb.x - float64(opts.virtualClip.Min.X)
		tY := vb.y - float64(opts.virtualClip.Min.Y)

		// filter points outside of clip + gutter
		if tX < -virtualWaterRadius || tY < -virtualWaterRadius || tX >= float64(opts.virtualClip.Max.X)+virtualWaterRadius || tY >= float64(opts.virtualClip.Max.Y)+virtualWaterRadius {
			continue
		}

		// marker in image coordinates
		iX := tX * virtualToActual
		iY := tY * virtualToActual

		// render marker
		Entry, ok := IDMap[vb.marker.tribeOrOwnerID]
		if !ok {
			Entry = FlagOwnerOutputHeader{
				TribeOrPlayerID: vb.marker.tribeOrOwnerID,
			}
		}

		switch vb.marker.markerType {
		case 0:
			Entry.LandClaims = append(Entry.LandClaims, ClaimFlagOutputEntry{X: uint16(iX), Y: uint16(iY)})
		case 1:
			Entry.WaterClaims = append(Entry.WaterClaims, ClaimFlagOutputEntry{X: uint16(iX), Y: uint16(iY)})
		}

		IDMap[vb.marker.tribeOrOwnerID] = Entry
	}

	// save the a tmp file
	dir := path.Dir(opts.filename)
	os.MkdirAll(path.Dir(opts.filename), os.ModePerm)
	tmpFilename := path.Join(dir, tempFileName("tmp_", ".map"))
	f, _ := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)

	const FileVerison uint16 = 2
	const CompressionType uint16 = 0x0001 //0x01 = Zlib compression

	//Simple Header
	FileVerisonBuff := make([]byte, 2)
	binary.LittleEndian.PutUint16(FileVerisonBuff, FileVerison)
	f.Write(FileVerisonBuff)

	CompressionTypeBuff := make([]byte, 2)
	binary.LittleEndian.PutUint16(CompressionTypeBuff, CompressionType)
	f.Write(CompressionTypeBuff)

	SrcImageWidthBuff := make([]byte, 2)
	binary.LittleEndian.PutUint16(SrcImageWidthBuff, SrcPixels)
	f.Write(SrcImageWidthBuff)

	DestImageWidthBuff := make([]byte, 2)
	binary.LittleEndian.PutUint16(DestImageWidthBuff, uint16(config.GameSize))
	f.Write(DestImageWidthBuff)

	OwnerIDCountBuff := make([]byte, 4)
	binary.LittleEndian.PutUint32(OwnerIDCountBuff, uint32(len(IDMap)))
	f.Write(OwnerIDCountBuff)

	IDList := make([]FlagOwnerOutputHeader, 0, len(IDMap))
	for _, v := range IDMap {
		IDList = append(IDList, v)
	}
	sort.Sort(ByTribeOrPlayerID(IDList))
	for _, k := range IDList {
		//Write Entry Header
		TribeOrPlayerIDBuff := make([]byte, 8)
		binary.LittleEndian.PutUint64(TribeOrPlayerIDBuff, k.TribeOrPlayerID)
		f.Write(TribeOrPlayerIDBuff)

		//TODO: Use (20 bits: 0xFFFFF //F FF FF) for some of these eventually
		LandClaimsCountBuff := make([]byte, 4)
		binary.LittleEndian.PutUint32(LandClaimsCountBuff, uint32(len(k.LandClaims)))
		f.Write(LandClaimsCountBuff)
		WaterClaimCountBuff := make([]byte, 4)
		binary.LittleEndian.PutUint32(WaterClaimCountBuff, uint32(len(k.WaterClaims)))
		f.Write(WaterClaimCountBuff)

		//WriteEntries
		for _, LandEntry := range k.LandClaims {
			LandXBuff := make([]byte, 2)
			binary.LittleEndian.PutUint16(LandXBuff, LandEntry.X)
			f.Write(LandXBuff)

			LandYBuff := make([]byte, 2)
			binary.LittleEndian.PutUint16(LandYBuff, LandEntry.Y)
			f.Write(LandYBuff)
		}
		for _, WaterEntry := range k.WaterClaims {
			WaterXBuff := make([]byte, 2)
			binary.LittleEndian.PutUint16(WaterXBuff, WaterEntry.X)
			f.Write(WaterXBuff)

			WaterYBuff := make([]byte, 2)
			binary.LittleEndian.PutUint16(WaterYBuff, WaterEntry.Y)
			f.Write(WaterYBuff)
		}
	}

	f.Close()

	// delete old file and rename tmp
	os.Remove(opts.filename)
	os.Rename(tmpFilename, opts.filename)

	uploadToS3(opts.filename)
}

// generateTiles creates all the tile images at the specified zoom level
func generateTiles(tilePath string, zoomLevel uint, markers []Marker, wg *sync.WaitGroup) {
	defer wg.Done()

	opts := MapOptions{}
	opts.actualPixels = config.TileSize
	opts.virtualPixels = config.TileSize * (1 << (config.MaxZoom - 1))

	qt := createQuadTree(&opts, markers)

	tiles := 1 << zoomLevel
	virtualPixelsPerTile := opts.virtualPixels / tiles

	for tileX := 0; tileX < tiles; tileX++ {
		for tileY := 0; tileY < tiles; tileY++ {
			minX := tileX * virtualPixelsPerTile
			maxX := minX + virtualPixelsPerTile - 1
			minY := tileY * virtualPixelsPerTile
			maxY := minY + virtualPixelsPerTile - 1
			opts.virtualClip = image.Rect(minX, minY, maxX, maxY)
			opts.filename = path.Join(tilePath, strconv.Itoa(int(zoomLevel)), strconv.Itoa(tileX), strconv.Itoa(tileY)+".png")
			generateImage(&opts, qt)
		}
	}
}

func generateGame(gamePath string, markers []Marker) {
	var servers int
	if config.ServersX >= config.ServersY {
		servers = config.ServersX
	} else {
		servers = config.ServersY
	}

	// common image options
	opts := MapOptions{}

	const BitsPerPixel uint16 = 32
	ChannelBlocksPerDimension := uint16(math.Floor(math.Sqrt(float64(BitsPerPixel))))
	CorrectedGameSize := int(config.GameSize) * int(ChannelBlocksPerDimension)

	opts.actualPixels = CorrectedGameSize
	opts.virtualPixels = CorrectedGameSize * servers

	qt := createQuadTree(&opts, markers)

	// generate world map
	opts.virtualClip = image.Rect(0, 0, opts.virtualPixels-1, opts.virtualPixels-1)
	opts.filename = path.Join(gamePath, "world.map")
	generateCompressedFile(&opts, qt)
}

func fetchClaimMarkers(client *redis.Client) ([]Marker, uint32) {
	var crcs []uint32
	var markers []Marker

	for x := 0; x < config.ServersX; x++ {
		for y := 0; y < config.ServersY; y++ {
			results, err := client.SMembers(fmt.Sprintf("territorymapdata:%d", x<<16|y)).Result()
			if err != nil {
				log.Printf("Warning! %v", err)
				continue
			}
			for _, rawString := range results {
				bytes := []byte(rawString)

				newCRC := crc32.ChecksumIEEE(bytes)
				crcs = append(crcs, newCRC)

				tid := binary.LittleEndian.Uint64(bytes[0:8])
				tx := binary.LittleEndian.Uint16(bytes[8:10])
				ty := binary.LittleEndian.Uint16(bytes[10:12])

				m := Marker{}
				m.serverX = x
				m.serverY = y
				m.tribeOrOwnerID = tid
				m.relX = float64(tx) / float64(math.MaxUint16)
				m.relY = float64(ty) / float64(math.MaxUint16)
				m.markerType = bytes[12]

				markers = append(markers, m)
			}
		}
	}

	// generate CRC32  for markers for rough "have they changed" check
	sort.Slice(crcs, func(i, j int) bool { return crcs[i] < crcs[j] })
	hash := crc32.NewIEEE()
	for _, crc := range crcs {
		binary.Write(hash, binary.LittleEndian, crc)
	}

	return markers, hash.Sum32()
}

func updateUrlsInRedis(client *redis.Client) {
	var endpoint string
	if len(config.AlternativeURL) > 0 {
		endpoint = config.AlternativeURL
	} else if len(config.Host) > 0 {
		endpoint = fmt.Sprintf("%s:%d", config.Host, config.Port)
	} else {
		endpoint = fmt.Sprintf("localhost:%d", config.Port)
	}
	tag := rand.Int31()
	fields := make(map[string]interface{})
	fields["world"] = fmt.Sprintf("http://%s/gameTiles/world.map?t=%d", endpoint, tag)

	result := client.HMSet("territory_urls", fields)
	if result.Val() != "OK" {
		log.Printf("Warning! %v", result.Val())
		return
	}
}

func notifyUrlsChanged(client *redis.Client) {
	client.Publish("GeneralNotifications:GlobalCommands", "RefreshTerrityoryUrls")
}

func tileBackgroundWorker(client *redis.Client) {
	tilePath := path.Join(config.WWWDir, "territoryTiles")
	previousCrc := uint32(1)

	for {
		log.Println("Getting markers for tiles")
		markers, crc := fetchClaimMarkers(client)
		if crc != previousCrc {
			previousCrc = crc

			log.Println("Starting tile generation")
			var wg sync.WaitGroup
			wg.Add(int(config.MaxZoom))
			for zoom := uint(0); zoom < config.MaxZoom; zoom++ {
				go generateTiles(tilePath, zoom, markers, &wg)
			}
			wg.Wait()
			log.Println("Finished tile generation")
		} else {
			log.Println("tile CRCs matched so skipping generation")
		}

		time.Sleep(time.Duration(config.FetchRateInSeconds) * time.Second)
	}
}

func gameBackgroundWorker(client *redis.Client, notifyClient *redis.Client) {
	gamePath := path.Join(config.WWWDir, "gameTiles")
	previousCrc := uint32(1)

	updateUrlsInRedis(client)
	notifyUrlsChanged(notifyClient)

	for {
		log.Println("Getting markers for game image")
		markers, crc := fetchClaimMarkers(client)
		if crc != previousCrc {
			previousCrc = crc

			log.Println("Generating game images")
			generateGame(gamePath, markers)

			updateUrlsInRedis(client)
			notifyUrlsChanged(notifyClient)
		} else {
			log.Println("game CRCs matched so skipping generation")
		}

		time.Sleep(time.Duration(config.FetchRateInSeconds) * time.Second)
	}
}

type fileHandlerWithCacheControl struct {
	fileServer http.Handler
}

func (f *fileHandlerWithCacheControl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "max-age=60")
	f.fileServer.ServeHTTP(w, r)
}

// Min helper faster than float math.Min in Go
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Max helper faster than float math.Max in Go
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func main() {
	var err error

	config, err = loadConfig("./config.json")
	if err != nil {
		log.Printf("Warning: %v", err)
		log.Println("Failed to read configuration file: config.json")
	}

	defaultDbCfg := config.getDatabaseByName("Default")
	defaultClient := redis.NewClient(&redis.Options{
		Addr:     defaultDbCfg.URL + ":" + strconv.Itoa(defaultDbCfg.Port),
		Password: defaultDbCfg.Password,
		DB:       0,
	})

	dbCfg := config.getDatabaseByName("TerritoryDB")
	dbClient := redis.NewClient(&redis.Options{
		Addr:     dbCfg.URL + ":" + strconv.Itoa(dbCfg.Port),
		Password: dbCfg.Password,
		DB:       0,
	})

	if config.EnableTileGeneration {
		go tileBackgroundWorker(dbClient)
	}
	if config.EnableGameGeneration {
		go gameBackgroundWorker(dbClient, defaultClient)
	}

	http.Handle("/", &fileHandlerWithCacheControl{fileServer: http.FileServer(http.Dir(config.WWWDir))})

	endpoint := fmt.Sprintf(":%d", config.Host, config.Port)
	log.Println("Listening on ", endpoint)
	log.Fatal(http.ListenAndServe(endpoint, nil))
}
