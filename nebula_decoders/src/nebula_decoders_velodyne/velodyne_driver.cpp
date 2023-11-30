#include "nebula_decoders/nebula_decoders_velodyne/velodyne_driver.hpp"

#include "nebula_decoders/nebula_decoders_velodyne/decoders/vlp16_decoder.hpp"
#include "nebula_decoders/nebula_decoders_velodyne/decoders/vlp32_decoder.hpp"
#include "nebula_decoders/nebula_decoders_velodyne/decoders/vls128_decoder.hpp"

namespace nebula
{
namespace drivers
{
VelodyneDriver::VelodyneDriver(
  const std::shared_ptr<drivers::VelodyneSensorConfiguration> & sensor_configuration,
  const std::shared_ptr<drivers::VelodyneCalibrationConfiguration> & calibration_configuration)
: will_reset_pointcloud_{true},
  millis_each_publish_{110}
{
  auto time_point_now_since_epoch = std::chrono::system_clock::now().time_since_epoch();
  long millis_since_epoch_now =
    std::chrono::duration_cast<std::chrono::milliseconds>(time_point_now_since_epoch).count();
  auto period = std::chrono::milliseconds(millis_each_publish_).count();
  millis_since_epoch_next_start_ =
    millis_since_epoch_now - (millis_since_epoch_now % period) + period;
  // initialize proper parser from cloud config's model and echo mode
  driver_status_ = nebula::Status::OK;
  std::cout << "sensor_configuration->sensor_model=" << sensor_configuration->sensor_model
            << std::endl;
  switch (sensor_configuration->sensor_model) {
    case SensorModel::UNKNOWN:
      driver_status_ = nebula::Status::INVALID_SENSOR_MODEL;
      break;
    case SensorModel::VELODYNE_VLS128:
      scan_decoder_.reset(
        new drivers::vls128::Vls128Decoder(sensor_configuration, calibration_configuration));
      break;
    case SensorModel::VELODYNE_VLP32:
    case SensorModel::VELODYNE_HDL64:
    case SensorModel::VELODYNE_HDL32:
      scan_decoder_.reset(
        new drivers::vlp32::Vlp32Decoder(sensor_configuration, calibration_configuration));
      break;
    case SensorModel::VELODYNE_VLP16:
      scan_decoder_.reset(
        new drivers::vlp16::Vlp16Decoder(sensor_configuration, calibration_configuration));
      break;
    default:
      driver_status_ = nebula::Status::INVALID_SENSOR_MODEL;
      break;
  }
}

Status VelodyneDriver::SetCalibrationConfiguration(
  const CalibrationConfigurationBase & calibration_configuration)
{
  throw std::runtime_error(
    "SetCalibrationConfiguration. Not yet implemented (" +
    calibration_configuration.calibration_file + ")");
}

std::tuple<drivers::NebulaPointCloudPtr, double> VelodyneDriver::ConvertScanToPointcloud(
  const std::shared_ptr<velodyne_msgs::msg::VelodyneScan> & velodyne_scan)
{
  std::tuple<drivers::NebulaPointCloudPtr, double> pointcloud;
  if (driver_status_ == nebula::Status::OK) {
    scan_decoder_->reset_pointcloud(velodyne_scan->packets.size());
    for (auto & packet : velodyne_scan->packets) {
      scan_decoder_->unpack(packet);
    }
    pointcloud = scan_decoder_->get_pointcloud();
  } else {
    std::cout << "not ok driver_status_ = " << driver_status_ << std::endl;
  }
  return pointcloud;
}

std::optional<std::tuple<drivers::NebulaPointCloudPtr, double>> VelodyneDriver::AccumulatePacketToPointcloud(
  const std::shared_ptr<velodyne_msgs::msg::VelodynePacket> & velodyne_packet)
{
  if (driver_status_ != nebula::Status::OK) {
    std::cout << "not ok driver_status_ = " << driver_status_ << std::endl;
    return std::nullopt;
  }
  std::tuple<drivers::NebulaPointCloudPtr, double> pointcloud;
  if (will_reset_pointcloud_) {
    scan_decoder_->reset_pointcloud(100);
    will_reset_pointcloud_ = false;
    count_packets_since_last_publish_ = 0;
  }
  scan_decoder_->unpack(*velodyne_packet);
  count_packets_since_last_publish_++;

  // put it behind a condition
  auto time_point_now_since_epoch = std::chrono::system_clock::now().time_since_epoch();
  long millis_since_epoch_now =
    std::chrono::duration_cast<std::chrono::milliseconds>(time_point_now_since_epoch).count();

  // Wait until the next_start time
  if (millis_since_epoch_now >= millis_since_epoch_next_start_) {
    pointcloud = scan_decoder_->get_pointcloud();
    std::cout << "Log at: " << millis_since_epoch_now << " ms" << std::endl;
    std::cout << "accumulated packet count: " << count_packets_since_last_publish_ << std::endl;
    count_packets_since_last_publish_ = 0;
    will_reset_pointcloud_ = true;
    // Calculate the next multiple of millis_each_publish_ ms
    auto period = std::chrono::milliseconds(millis_each_publish_).count();
    millis_since_epoch_next_start_ =
      millis_since_epoch_now - (millis_since_epoch_now % period) + period;
    return pointcloud;
  }
  return std::nullopt;
}
Status VelodyneDriver::GetStatus() { return driver_status_; }

}  // namespace drivers
}  // namespace nebula
