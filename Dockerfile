FROM golang:1.15.6

COPY . /go/src/localOffersLoader
WORKDIR /go/src/localOffersLoader/

RUN go get github.com/gorilla/mux && \
    go get github.com/lib/pq && \
    go get github.com/tealeg/xlsx && \
    go get github.com/stretchr/testify/assert && \
    go get github.com/imroc/req

RUN GOOS=linux GOARCH=amd64 go build -o offersLoader ./offersLoader.go

CMD ["./offersLoader"]
