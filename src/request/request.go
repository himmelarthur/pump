package main

import (
    "encoding/json"
    "fmt"
    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/postgres"
    "io/ioutil"
    "net/http"
    "os"
    "strconv"
    "time"
)

const (
    DB_HOST = "localhost"
    DB_USER = "Arthur"
    DB_PASSWORD = "foo"
    DB_NAME = "pump"
)

type Album struct {
    Title string `json:"#text"`
}

type Artist struct {
    Name string `json:"#text"`
}

type Date struct {
    Timestamp string `json:"uts"`
}

type ResponseTrack struct {
    Title string `json:"name"`
    Artist Artist `json:"artist"`
    Album Album `json:"album"`
    Date Date `json:"date"`
}

type Tracks struct {
    Tracks []ResponseTrack `json:"track"`
}

type RecentTracksResponse struct {
    TrackList Tracks `json:"recenttracks"`
}

type Track struct {
    gorm.Model
    Title string
    Artist string
    Album string
    ListenedAt time.Time
}

func checkErr(err error) {
    if err != nil {
        panic(err.Error())
    }
}

func main() {
    for page := 1; page <= 50; page++ {
        get_track_page(page)
    }
}

func save_tracks(db *gorm.DB, tracks []ResponseTrack) {
    var db_track Track
    for _, track := range tracks {
        timestamp, _ := strconv.ParseInt(track.Date.Timestamp, 10, 64)
        var track_date time.Time = time.Unix(timestamp, 0)
        db_track = Track{
            Title: track.Title,
            Artist: track.Artist.Name,
            Album: track.Album.Title,
            ListenedAt: track_date,
        }
        db.Create(&db_track)
        db.Save(&db_track)
    }
}

func parse_tracks(body []byte) (*RecentTracksResponse, error) {
    var response = new(RecentTracksResponse)
    err := json.Unmarshal(body, &response)
    checkErr(err)
    return response, err
}

func connect() (db *gorm.DB) {
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME := os.Getenv("PUMP_DB_HOST"), os.Getenv("PUMP_DB_USER"), os.Getenv("PUMP_DB_PASSWORD"), os.Getenv("PUMP_DB_NAME")
    db, err := gorm.Open("postgres", fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME))
    checkErr(err)
    return
}

func get_track_page(page int) {
    var API_KEY string = os.Getenv("LASTFM_API_KEY")
    var url string = fmt.Sprintf("http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=ArthurHimmel&api_key=%s&format=json&limit=10&page=%d", API_KEY, page)
    res, err := http.Get(url)
    checkErr(err)
    body, err := ioutil.ReadAll(res.Body)
    checkErr(err)
    tracks_response, err := parse_tracks([]byte(body))
    checkErr(err)

    var db *gorm.DB = connect()
    defer db.Close()

    db.AutoMigrate(&Track{})
    save_tracks(db, tracks_response.TrackList.Tracks)
}
