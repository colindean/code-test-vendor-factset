package cx.cad.codetests.vendor_factset

import com.github.vickumar1981.stringdistance.StringConverter._

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
trait Check {
  def check(entity: Entity, vendor: Vendor): Option[CheckMatch]
}
object NameChecker extends Check {
  val NameCheckJaroWinklerDistanceThreshold = 0.98

  override def check(entity: Entity, vendor: Vendor): Option[CheckMatch] = {
    //println(s"Checking entity ${entity.id} and vendor ${vendor.id}…")
    val entityNames = List(
      entity.coverageData(FieldNames.Factset.Entity.Name),
      entity.coverageData(FieldNames.Factset.Entity.ProperName))

    val vendorName = vendor.vendorData("name")
    val vendorNameNoPunct = removePunctuation(vendorName)
    val variants = List(
      vendorName,
      vendorNameNoPunct,
      vendorName.toUpperCase,
    )

    /*println(s"Comparing entitynames ${entityNames} with vendors ${variants}")
    Thread.sleep(2000)*/

    // exact matches
    val matches = entityNames.intersect(variants)

    if(matches.nonEmpty) {
      val result = CheckMatch(
        entityId = entity.id,
        vendorId = vendor.id,
        confidence = 1.0,
        reason = s"Name exact match ($matches)")

      println(s"EXACT MATCH: $result")
      return Some(result)
    }

    // try string differencing
    //val jwDist: Double = JaroWinkler.score(
    //  entityNames.head, vendorNameNoPunct)
    //implicit works better?? wtf
    val jwDist: Double = entityNames.head.jaroWinkler(vendorNameNoPunct)

    if(jwDist > NameCheckJaroWinklerDistanceThreshold) {
      val result = CheckMatch(
        entityId = entity.id,
        vendorId = vendor.id,
        confidence = jwDist,
        reason = s"Name jw exceeding ${NameCheckJaroWinklerDistanceThreshold}"
      )
      println(s"JW MATCH: (${result})")
      return Some(result)
    }

    None
  }

  def removePunctuation(string: String): String = {
    string.replaceAll("\\p{Punct}", "")
  }
}

case class CheckMatch(
                       vendorId: VendorId,
                       entityId: EntityId,
                       confidence: Double,
                       reason: String) {
  def asCsvList: List[String] = List(vendorId.id, entityId.id, confidence.toString, reason)
}
object CheckMatch {
  // ideally, this should come from some kind of introspection on the case class
  // not going to dig into that right now
  def headers = List("vendorId", "entityId", "confidence", "reason")
}
