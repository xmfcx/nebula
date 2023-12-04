#ifndef NEBULA_VelodyneHwInterfaceRosWrapper_H
#define NEBULA_VelodyneHwInterfaceRosWrapper_H

#include "nebula_common/nebula_common.hpp"
#include "nebula_common/nebula_status.hpp"
#include "nebula_common/velodyne/velodyne_common.hpp"
#include "nebula_decoders/nebula_decoders_velodyne/velodyne_driver.hpp"
#include "nebula_hw_interfaces/nebula_hw_interfaces_velodyne/velodyne_hw_interface.hpp"
#include "nebula_ros/common/nebula_driver_ros_wrapper_base.hpp"
#include "nebula_ros/common/nebula_hw_interface_ros_wrapper_base.hpp"

#include <ament_index_cpp/get_package_prefix.hpp>
#include <diagnostic_updater/diagnostic_updater.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_components/register_node_macro.hpp>

#include <velodyne_msgs/msg/velodyne_packet.hpp>
#include <velodyne_msgs/msg/velodyne_scan.hpp>

#include <curl/curl.h>
#include <readerwriterqueue/readerwritercircularbuffer.h>

#include <future>
#include <mutex>
#include <data_tamer/data_tamer.hpp>
#include <data_tamer/sinks/mcap_sink.hpp>
#include <data_tamer/channel.hpp>

namespace nebula
{
namespace ros
{
/// @brief Get parameter from rclcpp::Parameter
/// @tparam T
/// @param p Parameter from rclcpp parameter callback
/// @param name Target parameter name
/// @param value Corresponding value
/// @return Whether the target name existed
template <typename T>
bool get_param(const std::vector<rclcpp::Parameter> & p, const std::string & name, T & value)
{
  auto it = std::find_if(p.cbegin(), p.cend(), [&name](const rclcpp::Parameter & parameter) {
    return parameter.get_name() == name;
  });
  if (it != p.cend()) {
    value = it->template get_value<T>();
    return true;
  }
  return false;
}

/// @brief Hardware interface ros wrapper of velodyne driver
class VelodyneHwInterfaceRosWrapper final : public rclcpp::Node, NebulaHwInterfaceWrapperBase, NebulaDriverRosWrapperBase
{
  drivers::VelodyneHwInterface hw_interface_;
  Status interface_status_;

  drivers::VelodyneSensorConfiguration sensor_configuration_;
  drivers::VelodyneCalibrationConfiguration calibration_configuration_;

  /// @brief Received Velodyne message publisher
  rclcpp::Publisher<velodyne_msgs::msg::VelodyneScan>::SharedPtr velodyne_scan_pub_;

  /// @brief Initializing hardware interface ros wrapper
  /// @param sensor_configuration SensorConfiguration for this driver
  /// @return Resulting status
  Status InitializeHwInterface(
    const drivers::SensorConfigurationBase & sensor_configuration) override;
  /// @brief Callback for receiving VelodyneScan
  /// @param scan_buffer Received VelodyneScan
  void ReceiveScanDataCallback(std::unique_ptr<velodyne_msgs::msg::VelodyneScan> scan_buffer);


  std::shared_ptr<drivers::VelodyneDriver> driver_ptr_;
  Status wrapper_status_;
  rclcpp::Subscription<velodyne_msgs::msg::VelodyneScan>::SharedPtr velodyne_scan_sub_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr nebula_points_pub_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr aw_points_ex_pub_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr aw_points_base_pub_;

  std::shared_ptr<drivers::CalibrationConfigurationBase> calibration_cfg_ptr_;
  std::shared_ptr<drivers::SensorConfigurationBase> sensor_cfg_ptr_;

  using TypeQueue = moodycamel::BlockingReaderWriterCircularBuffer<std::unique_ptr<velodyne_msgs::msg::VelodyneScan>>;
  std::unique_ptr<TypeQueue> queue_;
  std::future<void> future_worker_;
  std::mutex mtx_queue_cv_;
  std::condition_variable cv_queue_;

  std::shared_ptr<DataTamer::MCAPSink> mcap_sink_;
  std::shared_ptr<DataTamer::LogChannel> log_channel_;
  int count_ = 0;

  void WorkerConsumer();

  /// @brief Initializing ros wrapper
  /// @param sensor_configuration SensorConfiguration for this driver
  /// @param calibration_configuration CalibrationConfiguration for this driver
  /// @return Resulting status
  Status InitializeDriver(
    std::shared_ptr<drivers::SensorConfigurationBase> sensor_configuration,
    std::shared_ptr<drivers::CalibrationConfigurationBase> calibration_configuration) override;

  /// @brief Get configurations from ros parameters
  /// @param sensor_configuration Output of SensorConfiguration
  /// @param calibration_configuration Output of CalibrationConfiguration
  /// @return Resulting status
  Status GetParameters(
    drivers::VelodyneSensorConfiguration & sensor_configuration,
    drivers::VelodyneCalibrationConfiguration & calibration_configuration);

  /// @brief Convert seconds to chrono::nanoseconds
  /// @param seconds
  /// @return chrono::nanoseconds
  static inline std::chrono::nanoseconds SecondsToChronoNanoSeconds(const double seconds)
  {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::duration<double>(seconds));
  }

  /***
   * Publishes a sensor_msgs::msg::PointCloud2 to the specified publisher
   * @param pointcloud unique pointer containing the point cloud to publish
   * @param publisher
   */
  void PublishCloud(
    std::unique_ptr<sensor_msgs::msg::PointCloud2> pointcloud,
    const rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr & publisher);

public:
  explicit VelodyneHwInterfaceRosWrapper(const rclcpp::NodeOptions & options);
  /// @brief Start point cloud streaming (Call CloudInterfaceStart of HwInterface)
  /// @return Resulting status
  Status StreamStart() override;
  /// @brief Stop point cloud streaming (not used)
  /// @return Resulting status
  Status StreamStop() override;
  /// @brief Shutdown (not used)
  /// @return Resulting status
  Status Shutdown() override;
  /// @brief Get configurations from ros parameters
  /// @param sensor_configuration Output of SensorConfiguration
  /// @return Resulting status
  Status GetParameters(drivers::VelodyneSensorConfiguration & sensor_configuration);

  /// @brief Callback for VelodyneScan subscriber
  /// @param scan_msg Received VelodyneScan
  void ReceiveScanMsgCallback(const std::unique_ptr<velodyne_msgs::msg::VelodyneScan> scan_msg);
  /// @brief Get current status of this driver
  /// @return Current status
  Status GetStatus();

private:  // ROS Diagnostics
  /*
diagnostic_updater::Updater diagnostics_updater_;
void InitializeVelodyneDiagnostics();
*/

  /// @brief Get value from property_tree
  /// @param pt property_tree
  /// @param key Pey string
  /// @return Value
  std::string GetPtreeValue(
    std::shared_ptr<boost::property_tree::ptree> pt, const std::string & key);
  /*
  rclcpp::TimerBase::SharedPtr diagnostics_diag_timer_;
*/
  std::shared_ptr<boost::property_tree::ptree> current_diag_tree;
  /*
  void OnVelodyneDiagnosticsTimer();
  void VelodyneCheckTopHv(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopAdTemp(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopLm20Temp(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopPwr5v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopPwr25v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopPwr33v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopPwrRaw(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckTopPwrVccint(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotIOut(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwr12v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotLm20Temp(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwr5v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwr25v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwr33v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwrVIn(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckBotPwr125v(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckVhv(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckAdcNf(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckAdcStats(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckIxe(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckAdctpStat(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  rclcpp::TimerBase::SharedPtr diagnostics_status_timer_;
*/
  std::shared_ptr<boost::property_tree::ptree> current_status_tree;
  /*
  void OnVelodyneStatusTimer();
  void VelodyneCheckGpsPpsState(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckGpsPosition(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckMotorState(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckMotorRpm(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckMotorLock(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckMotorPhase(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);
  void VelodyneCheckLaserState(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);

  void VelodyneCheckSnapshot(diagnostic_updater::DiagnosticStatusWrapper & diagnostics);

  void OnVelodyneSnapshotTimer();
  rclcpp::TimerBase::SharedPtr diagnostics_snapshot_timer_;
  rclcpp::TimerBase::SharedPtr diagnostics_update_timer_;
*/
  std::shared_ptr<std::string> current_snapshot;
  std::shared_ptr<boost::property_tree::ptree> current_snapshot_tree;
  std::shared_ptr<rclcpp::Time> current_snapshot_time;
  //  rclcpp::Time current_snapshot_time;
  //  std::shared_ptr<uint8_t> current_diag_status;
  uint8_t current_diag_status;

  uint16_t diag_span_;
  std::mutex mtx_diag;
  std::mutex mtx_status;
  std::mutex mtx_config_;

  void curl_callback(std::string err, std::string body);
  /*
  const char* key_volt_temp_top_hv;
  const char* key_volt_temp_top_ad_temp;
  const char* key_volt_temp_top_lm20_temp;
  const char* key_volt_temp_top_pwr_5v;
  const char* key_volt_temp_top_pwr_2_5v;
  const char* key_volt_temp_top_pwr_3_3v;
  const char* key_volt_temp_top_pwr_raw;
  const char* key_volt_temp_top_pwr_vccint;
  const char* key_volt_temp_bot_i_out;
  const char* key_volt_temp_bot_pwr_1_2v;
  const char* key_volt_temp_bot_lm20_temp;
  const char* key_volt_temp_bot_pwr_5v;
  const char* key_volt_temp_bot_pwr_2_5v;
  const char* key_volt_temp_bot_pwr_3_3v;
  const char* key_volt_temp_bot_pwr_v_in;
  const char* key_volt_temp_bot_pwr_1_25v;
  const char* key_vhv;
  const char* key_adc_nf;
  const char* key_adc_stats;
  const char* key_ixe;
  const char* key_adctp_stat;
  const char* key_status_gps_pps_state;
  const char* key_status_gps_pps_position;
  const char* key_status_motor_state;
  const char* key_status_motor_rpm;
  const char* key_status_motor_lock;
  const char* key_status_motor_phase;
  const char* key_status_laser_state;
*/
  const char * not_supported_message;

  OnSetParametersCallbackHandle::SharedPtr set_param_res_;
  /// @brief rclcpp parameter callback
  /// @param parameters Received parameters
  /// @return SetParametersResult
  rcl_interfaces::msg::SetParametersResult paramCallback(
    const std::vector<rclcpp::Parameter> & parameters);
  /// @brief Updating rclcpp parameter
  /// @return SetParametersResult
  std::vector<rcl_interfaces::msg::SetParametersResult> updateParameters();

  //  rclcpp::callback_group::CallbackGroup::SharedPtr cbg_;
  rclcpp::CallbackGroup::SharedPtr cbg_r_;
  rclcpp::CallbackGroup::SharedPtr cbg_m_;
};

}  // namespace ros
}  // namespace nebula

#endif  // NEBULA_VelodyneHwInterfaceRosWrapper_H
