import java.sql.Timestamp
import java.time.Instant
import java.util.Properties

import com.google.transit.realtime.GtfsRealtime.TripUpdate
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.connector.jdbc.JdbcSink

object TripUpdateFlinkJob {

  class TripUpdateDeserializationSchema extends AbstractDeserializationSchema[TripUpdate] {
    override def deserialize(message: Array[Byte]): TripUpdate = {
      TripUpdate.parseFrom(message)
    }
  }

  case class TripUpdateRow(
    trip_id: String,
    start_date: String,
    route_id: String,
    timestamp: Timestamp
  )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS",
      throw new RuntimeException("KAFKA_BOOTSTRAP_SERVERS env var is missing"))
    val kafkaGroupId = sys.env.getOrElse("KAFKA_GROUP_ID", "trip_update_group")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "trip_updates")

    val dbHost = sys.env.getOrElse("POSTGRES_HOST", "timescaledb")
    val dbPort = sys.env.getOrElse("POSTGRES_PORT", "5432")
    val dbName = sys.env.getOrElse("POSTGRES_DB", "transit_delays")
    val dbUser = sys.env.getOrElse("POSTGRES_USER", "transit_user")
    val dbPass = sys.env.get("POSTGRES_PASSWORD")
      .getOrElse(throw new RuntimeException("POSTGRES_PASSWORD env var is missing"))

    val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBootstrapServers)
    props.setProperty("group.id", kafkaGroupId)

    val consumer = new FlinkKafkaConsumer[TripUpdate](
      kafkaTopic,
      new TripUpdateDeserializationSchema,
      props
    )

    val stream = env
      .addSource(consumer)
      .map { update =>
        val trip = update.getTrip
        TripUpdateRow(
          trip.getTripId,
          trip.getStartDate,
          trip.getRouteId,
          Timestamp.from(Instant.now())
        )
      }

    stream.addSink(
      JdbcSink.sink[TripUpdateRow](
        """
          INSERT INTO trip_updates (trip_id, start_date, route_id, timestamp)
          VALUES (?, ?, ?, ?)
        """,
        (ps, t) => {
          ps.setString(1, t.trip_id)
          ps.setString(2, t.start_date)
          ps.setString(3, t.route_id)
          ps.setTimestamp(4, t.timestamp)
        },
        new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl(jdbcUrl)
          .withDriverName("org.postgresql.Driver")
          .withUsername(dbUser)
          .withPassword(dbPass)
          .build()
      )
    )

    env.execute("Trip Update Flink Job")
  }
}
