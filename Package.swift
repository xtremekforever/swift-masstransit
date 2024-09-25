// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-masstransit",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "MassTransit",
            targets: ["MassTransit"])
    ],
    dependencies: [
        .package(
            url: "https://github.com/xtremekforever/swift-rabbitmq", branch: "feature/11-improve-efficiency-lifecycle"),
        .package(url: "https://github.com/apple/swift-distributed-tracing-extras.git", from: "1.0.0-beta.1"),

    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "MassTransit",
            dependencies: [
                .product(name: "RabbitMq", package: "swift-rabbitmq"),
                .product(name: "TracingOpenTelemetrySemanticConventions", package: "swift-distributed-tracing-extras"),
            ]
        ),
        .executableTarget(
            name: "PublishConsume",
            dependencies: [
                "MassTransit"
            ],
            path: "Sources/Examples/PublishConsume"
        ),
        .executableTarget(
            name: "RequestResponse",
            dependencies: [
                "MassTransit"
            ],
            path: "Sources/Examples/RequestResponse"
        ),
    ]
)

// Enable strict concurrency checking for all targets
for target in package.targets {
    var settings = target.swiftSettings ?? []
    settings.append(.enableExperimentalFeature("StrictConcurrency"))
    target.swiftSettings = settings
}
