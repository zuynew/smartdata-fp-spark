package ru.ozon

import derevo.derive
import derevo.pureconfig.pureconfigReader

object config {

  @derive(pureconfigReader)
  case class AppConfig(vault: Vault)

  @derive(pureconfigReader)
  case class Vault(address: String, token: String)

}
