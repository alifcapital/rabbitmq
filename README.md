### Overview

Main goal of this repository is to be able automatically re-connect to rabbitmq and resume consuming messages.

- [x] auto-reconnection
- [x] restore-consumers

All connection, publishing, consuming logic here are just handy wrappers of "github.com/rabbitmq/amqp091-go". I assume
that you have a good understanding of rabbitmq and leave you a full example of how it may look like in your
project in `cmd` directory. Otherwise, start learning here: https://www.rabbitmq.com/getstarted.html

### Install

```shell
go get -u github.com/alifcapital/rabbitmq
```

### Contributing

Pull requests are welcome. For any changes, please open an issue first to discuss what you would like to change.
