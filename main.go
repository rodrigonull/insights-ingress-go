package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/redhatinsights/insights-ingress-go/announcers"
	"github.com/redhatinsights/insights-ingress-go/config"
	l "github.com/redhatinsights/insights-ingress-go/logger"
	"github.com/redhatinsights/insights-ingress-go/pipeline"
	"github.com/redhatinsights/insights-ingress-go/queue"
	"github.com/redhatinsights/insights-ingress-go/stage"
	"github.com/redhatinsights/insights-ingress-go/stage/s3"
	"github.com/redhatinsights/insights-ingress-go/upload"
	"github.com/redhatinsights/insights-ingress-go/validators"
	"github.com/redhatinsights/insights-ingress-go/validators/kafka"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redhatinsights/platform-go-middlewares/identity"
	"go.uber.org/zap"
)

func lubDub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("lubdub"))
}

func main() {
	cfg := config.Get()
	l.InitLogger()
	r := chi.NewRouter()
	r.Use(
		middleware.RequestID,
		middleware.RealIP,
		middleware.Logger,
		middleware.Recoverer,
	)
	if cfg.Auth {
		r.Use(
			identity.EnforceIdentity,
		)
	}

	valCh := make(chan *validators.Response)
	invCh := make(chan *validators.Response)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var p *pipeline.Pipeline

	if cfg.Simulate {
		p = &pipeline.Pipeline{
			Stager: &stage.Simulation{Delay: cfg.SimulationStageDelay * time.Millisecond},
			Validator: validators.NewSimulation(&validators.Simulation{
				CallDelay:   cfg.SimulationValidateCallDelay * time.Millisecond,
				Delay:       cfg.SimulationValidateDelay * time.Millisecond,
				ValidChan:   valCh,
				InvalidChan: invCh,
				Context:     ctx,
			}),
			Announcer:   &announcers.Fake{},
			ValidChan:   valCh,
			InvalidChan: invCh,
		}
	} else {
		p = &pipeline.Pipeline{
			Stager: s3.WithSession(&s3.S3Stager{
				Bucket:   cfg.StageBucket,
				Rejected: cfg.RejectBucket,
			}),
			Validator: kafka.New(&kafka.Config{
				Brokers:         cfg.KafkaBrokers,
				GroupID:         cfg.KafkaGroupID,
				ValidationTopic: cfg.KafkaValidationTopic,
				ValidChan:       valCh,
				InvalidChan:     invCh,
				Context:         ctx,
			}, cfg.ValidTopics...),
			Announcer: announcers.NewKafkaAnnouncer(&queue.ProducerConfig{
				Brokers: cfg.KafkaBrokers,
				Topic:   cfg.KafkaAvailableTopic,
			}),
			ValidChan:   valCh,
			InvalidChan: invCh,
		}
	}

	pipelineClosed := make(chan struct{})
	go p.Start(context.Background(), pipelineClosed)

	r.Route("/api/ingress/v1", func(r chi.Router) {
		r.Get("/", lubDub)
		r.Post("/upload", upload.NewHandler(p))
	})
	r.Get("/", lubDub)
	r.Handle("/metrics", promhttp.Handler())
	l.Log.Info("Starting service", zap.Int("port", cfg.Port))

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: r,
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		if err := srv.Shutdown(context.Background()); err != nil {
			l.Log.Fatal("HTTP Server Shutdown failed", zap.Error(err))
		}
		l.Log.Info("Canceling validation consumer")
		cancel()
		l.Log.Info("Waiting for validation pipeline to close")
		<-pipelineClosed
		l.Log.Info("Pipeline finished, canceling announcer")
		p.Announcer.Stop()
		l.Log.Info("Announcer finished, shutting down")
		close(idleConnsClosed)
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		l.Log.Fatal("Service stopped", zap.Error(err))
	}

	<-idleConnsClosed
	l.Log.Info("Everything has shut down, goodbye")
}
