// swift-tools-version: 6.0
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
        .package(url: "https://github.com/xtremekforever/swift-rabbitmq.git", from: "0.3.1"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.48.0"),
        .package(url: "https://github.com/apple/swift-distributed-tracing-extras.git", from: "1.0.0-beta.1"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "MassTransit",
            dependencies: [
                .product(name: "RabbitMq", package: "swift-rabbitmq"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "TracingOpenTelemetrySemanticConventions", package: "swift-distributed-tracing-extras"),
            ]
        ),
        .testTarget(
            name: "Tests",
            dependencies: [
                "MassTransit"
            ]
        ),
        .executableTarget(
            name: "PublishConsume",
            dependencies: [
                "MassTransit",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            path: "Sources/Examples/PublishConsume"
        ),
        .executableTarget(
            name: "RequestResponse",
            dependencies: [
                "MassTransit",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            path: "Sources/Examples/RequestResponse"
        ),
        .executableTarget(
            name: "MultiMessageConsumer",
            dependencies: [
                "MassTransit",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            path: "Sources/Examples/MultiMessageConsumer"
        ),
    ]
)
