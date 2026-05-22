package appfx

import "go.uber.org/fx"

// Module keeps the compatibility bundle for the service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	AppModule,
	StorageModule,
	RepoModule,
	MessagingModule,
	GigClientModule,
	ClientModule,
	ServiceModule,
	PresentationModule,
)
