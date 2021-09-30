package ru.ozon

import java.time.Instant

object data {
  case class Client(id: Long, name: String)

  // Time MSK
  case class Call(id: Long, clientId: String, startAt: Instant, endAt: Instant)

  // Time UTC
  case class Chat(id: Long, clientId: String, startAt: Instant, endAt: Instant, messages: List[ChatMessage])

  // Time UTC another format
  case class ChatMessage(id: Long, text: String, sendAt: Instant)

  case class ClientCall(id: Long, clientId: String, startAt: Instant, endAt: Instant, name: String)
  case class ClientChat(
      id: Long,
      clientId: String,
      startAt: Instant,
      endAt: Instant,
      messages: List[ChatMessage],
      name: String
  )
}
