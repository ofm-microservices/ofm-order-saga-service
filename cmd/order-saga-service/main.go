package main

import (
	appfx "order-saga-service/internal/fx"

	"go.uber.org/fx"
)

var newApp = fx.New
var runApp = (*fx.App).Run

func main() {
	runApp(newApp(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.RepoModule,
		appfx.MessagingModule,
		appfx.GigClientModule,
		appfx.ClientModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	))
}
