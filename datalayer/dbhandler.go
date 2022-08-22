package datalayer

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	queryGetBooks       = "SELECT * FROM books"
	queryGetBookByTitle = "SELECT * FROM books WHERE title=?"
)

type Book struct {
	Id        int64  `json:"id"`
	Title     string `json:"title"`
	Author    string `json:"author"`
	Published string `json:"published"`
}

type SqlHandler struct {
	db *sql.DB
}

func createDbConnection(cs string) (*SqlHandler, error) {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8",
		"root",
		"",
		"localhost",
		"grpc_books",
	)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	return &SqlHandler{
		db: db,
	}, nil
}

func (h *SqlHandler) GetBooks() ([]Book, error) {
	rows, err := h.db.Query(queryGetBooks)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	books := []Book{}
	for rows.Next() {
		b := Book{}
		err = rows.Scan(&b.Id, &b.Title, &b.Author, &b.Published)
		if err != nil {
			log.Println(err)
			continue
		}
		books = append(books, b)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return books, nil
}

func (h *SqlHandler) GetBookByTitle(t string) (Book, error) {
	row := h.db.QueryRow(queryGetBookByTitle, t)
	b := Book{}

	err := row.Scan(&b.Id, &b.Title, &b.Author, &b.Published)
	if err != nil {
		return b, err
	}
	return b, nil
}
