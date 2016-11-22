package main

import (
    "encoding/json"
    "fmt"
    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/postgres"
    _ "github.com/jinzhu/gorm/dialects/sqlite"
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

type Import struct {
    gorm.Model
    NumberOfTracks int
    ImportLastDate time.Time
}

func checkErr(err error) {
    if err != nil {
        panic(err.Error())
    }
}

func connect() (*gorm.DB) {
    var DB_TYPE string = os.Getenv("PUMP_DB_TYPE")
    if DB_TYPE == "postgres" {
        return connect_postgres()
    } else {
        return connect_sqlite()
    }
}

func connect_postgres() (db *gorm.DB) {
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME := os.Getenv("PUMP_DB_HOST"), os.Getenv("PUMP_DB_USER"), os.Getenv("PUMP_DB_PASSWORD"), os.Getenv("PUMP_DB_NAME")
    db, err := gorm.Open("postgres", fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME))
    checkErr(err)
    return
}

func connect_sqlite() (db *gorm.DB) {
    db, err := gorm.Open("sqlite3", "/tmp/pump.db")
    checkErr(err)
    return
}

func main() {
    var db *gorm.DB = connect()
    defer db.Close()
    db.AutoMigrate(&Track{}, &Import{})

    pump(db)
}

func pump(db *gorm.DB) {

    last_import := Import{}
    db.Last(&last_import)

    tracks := make([]ResponseTrack, 0)

    for page := 1; page <= 50; page++ {
        fmt.Println("Fetching page", page)
        tracks = append(tracks, get_track_page(page)...)
    }
    fmt.Println("Saving tracks...", len(tracks))
    err := save_tracks(db, tracks, last_import.ImportLastDate)
    checkErr(err)
    fmt.Println("Tracks saved!")
    if len(tracks) > 0 {
        var new_import Import = Import{
            ImportLastDate: listen_time(tracks[0]),
            NumberOfTracks: len(tracks),
        }
        db.Create(&new_import)
    }

}

func save_tracks(db *gorm.DB, tracks []ResponseTrack, last_import_date time.Time) (err error){
    tx := db.Begin()
    var db_track Track
    for _, track := range tracks {
        var track_date time.Time = listen_time(track)
        if track_date.Before(last_import_date) || track_date.Equal(last_import_date) {
            tx.Commit()
            return nil
        }
        db_track = Track{
            Title: track.Title,
            Artist: track.Artist.Name,
            Album: track.Album.Title,
            ListenedAt: track_date,
        }
        if err := tx.Create(&db_track).Error; err != nil {
            tx.Rollback()
            return err
        }
    }
    tx.Commit()
    return nil
}

func listen_time(track ResponseTrack) time.Time {
    timestamp, _ := strconv.ParseInt(track.Date.Timestamp, 10, 64)
    var time time.Time = time.Unix(timestamp, 0)
    return time
}

func parse_tracks(body []byte) (*RecentTracksResponse, error) {
    var response = new(RecentTracksResponse)
    err := json.Unmarshal(body, &response)
    checkErr(err)
    return response, err
}

func get_track_page(page int) []ResponseTrack{
    var API_KEY string = os.Getenv("LASTFM_API_KEY")
    var url string = fmt.Sprintf("http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=ArthurHimmel&api_key=%s&format=json&limit=10&page=%d", API_KEY, page)
    res, err := http.Get(url)
    checkErr(err)
    body, err := ioutil.ReadAll(res.Body)
    checkErr(err)
    tracks_response, err := parse_tracks([]byte(body))
    checkErr(err)
    return tracks_response.TrackList.Tracks
}
