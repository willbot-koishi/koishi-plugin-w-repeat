# koishi-plugin-w-repeat

[![npm](https://img.shields.io/npm/v/koishi-plugin-w-repeat?style=flat-square)](https://www.npmjs.com/package/koishi-plugin-w-repeat)

WillBot: Advanced repeating functions

New repeat records reference canonical messages from
`koishi-plugin-w-message-db` instead of storing message content and image
base64 data again. The message-db plugin is therefore a required service.

## Roadmap

- [x] Repetition of images
  - [ ] Fuzzy matching
- [x] Suspension and resumption of repetition (A A A B A A B A A A ...)
- [x] Real repetition starter (\*A B A A A ...)
  - [ ] Repetition with sliced source (ABC B B B ...)
- Better graph
  - [x] Data normalization for flow graph
