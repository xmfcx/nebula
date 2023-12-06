#include "nebula_ros/velodyne/velodyne_hw_interface_ros_wrapper.hpp"


namespace nebula
{
namespace ros
{
VelodyneHwInterfaceRosWrapper::VelodyneHwInterfaceRosWrapper(const rclcpp::NodeOptions & options)
: rclcpp::Node("velodyne_hw_interface_ros_wrapper", options), hw_interface_()
{
  mcap_sink_ = std::make_shared<DataTamer::MCAPSink>("/home/mfc/data/point-cloud-sync-data-tamer/after.mcap");
  log_channel_ = DataTamer::LogChannel::create("after");
  log_channel_->addDataSink(mcap_sink_);

  drivers::VelodyneCalibrationConfiguration calibration_configuration;
  drivers::VelodyneSensorConfiguration sensor_configuration;

  wrapper_status_ = GetParameters(sensor_configuration, calibration_configuration);
  if (Status::OK != wrapper_status_) {
    RCLCPP_ERROR_STREAM(this->get_logger(), this->get_name() << " Error:" << wrapper_status_);
    return;
  }
  RCLCPP_INFO_STREAM(this->get_logger(), this->get_name() << ". Starting...");

  calibration_cfg_ptr_ =
    std::make_shared<drivers::VelodyneCalibrationConfiguration>(calibration_configuration);

  sensor_cfg_ptr_ = std::make_shared<drivers::VelodyneSensorConfiguration>(sensor_configuration);

  RCLCPP_INFO_STREAM(this->get_logger(), this->get_name() << ". Driver ");
  wrapper_status_ = InitializeDriver(
    std::const_pointer_cast<drivers::SensorConfigurationBase>(sensor_cfg_ptr_),
    std::static_pointer_cast<drivers::CalibrationConfigurationBase>(calibration_cfg_ptr_));

  RCLCPP_INFO_STREAM(this->get_logger(), this->get_name() << "Wrapper=" << wrapper_status_);

//  velodyne_scan_sub_ = create_subscription<velodyne_msgs::msg::VelodyneScan>(
//    "velodyne_packets", rclcpp::SensorDataQoS(),
//    std::bind(&VelodyneHwInterfaceRosWrapper::ReceiveScanMsgCallback, this, std::placeholders::_1));
  nebula_points_pub_ = this->create_publisher<sensor_msgs::msg::PointCloud2>(
    "velodyne_points", rclcpp::SensorDataQoS());
  aw_points_base_pub_ =
    this->create_publisher<sensor_msgs::msg::PointCloud2>("aw_points", rclcpp::SensorDataQoS());
  aw_points_ex_pub_ =
    this->create_publisher<sensor_msgs::msg::PointCloud2>("aw_points_ex", rclcpp::SensorDataQoS());

  queue_ = std::make_unique<TypeQueue>(100);


  not_supported_message = "Not supported";

  if (mtx_config_.try_lock()) {
    interface_status_ = GetParameters(sensor_configuration_);
    mtx_config_.unlock();
  }
  if (Status::OK != interface_status_) {
    RCLCPP_ERROR_STREAM(this->get_logger(), this->get_name() << " Error:" << interface_status_);
    return;
  }

  hw_interface_.SetLogger(std::make_shared<rclcpp::Logger>(this->get_logger()));
  // Initialize sensor_configuration
  RCLCPP_INFO_STREAM(this->get_logger(), "Initialize sensor_configuration");
  std::shared_ptr<drivers::SensorConfigurationBase> sensor_cfg_ptr =
    std::make_shared<drivers::VelodyneSensorConfiguration>(sensor_configuration_);
  RCLCPP_INFO_STREAM(this->get_logger(), "hw_interface_.InitializeSensorConfiguration");
  hw_interface_.InitializeSensorConfiguration(
    std::static_pointer_cast<drivers::SensorConfigurationBase>(sensor_cfg_ptr));

  if (this->setup_sensor) {
    RCLCPP_INFO_STREAM(this->get_logger(), "hw_interface_.SetSensorConfiguration");
    hw_interface_.SetSensorConfiguration(
      std::static_pointer_cast<drivers::SensorConfigurationBase>(sensor_cfg_ptr));
    updateParameters();
  }

  // register scan callback and publisher
  hw_interface_.RegisterPacketCallback(std::bind(&VelodyneHwInterfaceRosWrapper::ReceivePacketCallback, this, std::placeholders::_1));
//  velodyne_scan_pub_ = this->create_publisher<velodyne_msgs::msg::VelodyneScan>(
//    "velodyne_packets",
//    rclcpp::SensorDataQoS(rclcpp::KeepLast(10)).best_effort().durability_volatile());

  if (this->setup_sensor) {
    set_param_res_ = this->add_on_set_parameters_callback(
      std::bind(&VelodyneHwInterfaceRosWrapper::paramCallback, this, std::placeholders::_1));
  }

  auto status = StreamStart();
  if (status == nebula::Status::OK) {
    RCLCPP_INFO_STREAM(get_logger(), "UDP Driver Started");
  } else {
    RCLCPP_ERROR_STREAM(get_logger(), status);
  }

  future_worker_ = std::async(std::launch::async, &VelodyneHwInterfaceRosWrapper::WorkerConsumer, this);
}

void VelodyneHwInterfaceRosWrapper::WorkerConsumer(){
  while (rclcpp::ok()) {
//    RCLCPP_INFO(get_logger(), "workerconsumer loopin");
    std::unique_lock<std::mutex> lock(mtx_queue_cv_);
    cv_queue_.wait(lock);
    std::unique_ptr<velodyne_msgs::msg::VelodynePacket> packet;
    if (queue_->try_dequeue(packet)) {
      ReceiveScanMsgCallback(std::move(packet));
//      RCLCPP_INFO(get_logger(), "queue size as consume: %d", queue_->size_approx());
    }
  }
}

Status VelodyneHwInterfaceRosWrapper::StreamStart()
{
  if (Status::OK == interface_status_) {
    interface_status_ = hw_interface_.CloudInterfaceStart();
  }
  return interface_status_;
}

Status VelodyneHwInterfaceRosWrapper::StreamStop() { return Status::OK; }
Status VelodyneHwInterfaceRosWrapper::Shutdown() { return Status::OK; }

Status VelodyneHwInterfaceRosWrapper::InitializeHwInterface(  // todo: don't think this is needed
  const drivers::SensorConfigurationBase & sensor_configuration)
{
  std::stringstream ss;
  ss << sensor_configuration;
  RCLCPP_DEBUG_STREAM(this->get_logger(), ss.str());
  return Status::OK;
}

Status VelodyneHwInterfaceRosWrapper::GetParameters(
  drivers::VelodyneSensorConfiguration & sensor_configuration)
{
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
//    this->declare_parameter<std::string>("sensor_model", "");
    sensor_configuration.sensor_model =
      nebula::drivers::SensorModelFromString(this->get_parameter("sensor_model").as_string());
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
//    this->declare_parameter<std::string>("return_mode", "", descriptor);
    sensor_configuration.return_mode =
      nebula::drivers::ReturnModeFromString(this->get_parameter("return_mode").as_string());
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("host_ip", "255.255.255.255", descriptor);
    sensor_configuration.host_ip = this->get_parameter("host_ip").as_string();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("sensor_ip", "192.168.1.201", descriptor);
    sensor_configuration.sensor_ip = this->get_parameter("sensor_ip").as_string();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
//    this->declare_parameter<std::string>("frame_id", "velodyne", descriptor);
    sensor_configuration.frame_id = this->get_parameter("frame_id").as_string();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<uint16_t>("data_port", 2368, descriptor);
    sensor_configuration.data_port = this->get_parameter("data_port").as_int();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<uint16_t>("gnss_port", 2369, descriptor);
    sensor_configuration.gnss_port = this->get_parameter("gnss_port").as_int();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "Angle where scans begin (degrees, [0.,360.]";
    rcl_interfaces::msg::FloatingPointRange range;
    range.set__from_value(0).set__to_value(360).set__step(0.01);
    descriptor.floating_point_range = {range};
//    this->declare_parameter<double>("scan_phase", 0., descriptor);
    sensor_configuration.scan_phase = this->get_parameter("scan_phase").as_double();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<uint16_t>("packet_mtu_size", 1500, descriptor);
    sensor_configuration.packet_mtu_size = this->get_parameter("packet_mtu_size").as_int();
  }

  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "range from 300 to 1200, in increments of 60";
    rcl_interfaces::msg::IntegerRange range;
    range.set__from_value(300).set__to_value(1200).set__step(1);
    descriptor.integer_range = {range};
    this->declare_parameter<uint16_t>("rotation_speed", 600, descriptor);
    sensor_configuration.rotation_speed = this->get_parameter("rotation_speed").as_int();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    rcl_interfaces::msg::IntegerRange range;
    range.set__from_value(0).set__to_value(360).set__step(1);
    descriptor.integer_range = {range};
//    this->declare_parameter<uint16_t>("cloud_min_angle", 0, descriptor);
    sensor_configuration.cloud_min_angle = this->get_parameter("cloud_min_angle").as_int();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    rcl_interfaces::msg::IntegerRange range;
    range.set__from_value(0).set__to_value(360).set__step(1);
    descriptor.integer_range = {range};
//    this->declare_parameter<uint16_t>("cloud_max_angle", 360, descriptor);
    sensor_configuration.cloud_max_angle = this->get_parameter("cloud_max_angle").as_int();
  }

  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_BOOL;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<bool>("setup_sensor", true, descriptor);
    this->setup_sensor = this->get_parameter("setup_sensor").as_bool();
  }

  if (sensor_configuration.sensor_model == nebula::drivers::SensorModel::UNKNOWN) {
    return Status::INVALID_SENSOR_MODEL;
  }
  if (sensor_configuration.return_mode == nebula::drivers::ReturnMode::UNKNOWN) {
    return Status::INVALID_ECHO_MODE;
  }
  if (sensor_configuration.frame_id.empty() || sensor_configuration.scan_phase > 360) {  // ||
    return Status::SENSOR_CONFIG_ERROR;
  }

  RCLCPP_INFO_STREAM(this->get_logger(), "SensorConfig:" << sensor_configuration);
  return Status::OK;
}

void VelodyneHwInterfaceRosWrapper::ReceivePacketCallback(
  std::unique_ptr<velodyne_msgs::msg::VelodynePacket> packet)
{
  if (!queue_->try_enqueue(std::move(packet))){
    queue_->try_pop();
    queue_->try_enqueue(std::move(packet));
  }
  cv_queue_.notify_one();
//  velodyne_scan_pub_->publish(*scan_buffer);
}

std::string VelodyneHwInterfaceRosWrapper::GetPtreeValue(
  std::shared_ptr<boost::property_tree::ptree> pt, const std::string & key)
{
  boost::optional<std::string> value = pt->get_optional<std::string>(key);
  if (value) {
    return value.get();
  } else {
    return not_supported_message;
  }
}

rcl_interfaces::msg::SetParametersResult VelodyneHwInterfaceRosWrapper::paramCallback(
  const std::vector<rclcpp::Parameter> & p)
{
  std::scoped_lock lock(mtx_config_);
  std::cout << "add_on_set_parameters_callback" << std::endl;
  std::cout << p << std::endl;
  std::cout << sensor_configuration_ << std::endl;

  drivers::VelodyneSensorConfiguration new_param{sensor_configuration_};
  std::cout << new_param << std::endl;
  std::string sensor_model_str;
  std::string return_mode_str;
  if (
    get_param(p, "sensor_model", sensor_model_str) ||
    get_param(p, "return_mode", return_mode_str) || get_param(p, "host_ip", new_param.host_ip) ||
    get_param(p, "sensor_ip", new_param.sensor_ip) ||
    get_param(p, "frame_id", new_param.frame_id) ||
    get_param(p, "data_port", new_param.data_port) ||
    get_param(p, "gnss_port", new_param.gnss_port) ||
    get_param(p, "scan_phase", new_param.scan_phase) ||
    get_param(p, "packet_mtu_size", new_param.packet_mtu_size) ||
    get_param(p, "rotation_speed", new_param.rotation_speed) ||
    get_param(p, "cloud_min_angle", new_param.cloud_min_angle) ||
    get_param(p, "cloud_max_angle", new_param.cloud_max_angle)) {  // ||

    if (0 < sensor_model_str.length())
      new_param.sensor_model = nebula::drivers::SensorModelFromString(sensor_model_str);
    if (0 < return_mode_str.length())
      new_param.return_mode = nebula::drivers::ReturnModeFromString(return_mode_str);

    sensor_configuration_ = new_param;
    // Update sensor_configuration
    RCLCPP_INFO_STREAM(this->get_logger(), "Update sensor_configuration");
    std::shared_ptr<drivers::SensorConfigurationBase> sensor_cfg_ptr =
      std::make_shared<drivers::VelodyneSensorConfiguration>(sensor_configuration_);
    RCLCPP_INFO_STREAM(this->get_logger(), "hw_interface_.SetSensorConfiguration");
    hw_interface_.SetSensorConfiguration(
      std::static_pointer_cast<drivers::SensorConfigurationBase>(sensor_cfg_ptr));
  }

  auto result = std::make_shared<rcl_interfaces::msg::SetParametersResult>();
  result->successful = true;
  result->reason = "success";

  std::cout << "add_on_set_parameters_callback success" << std::endl;

  return *result;
}

std::vector<rcl_interfaces::msg::SetParametersResult>
VelodyneHwInterfaceRosWrapper::updateParameters()
{
  std::scoped_lock lock(mtx_config_);
  std::cout << "!!!!!!!!!!!updateParameters!!!!!!!!!!!!" << std::endl;
  std::ostringstream os_sensor_model;
  os_sensor_model << sensor_configuration_.sensor_model;
  std::ostringstream os_return_mode;
  os_return_mode << sensor_configuration_.return_mode;
  std::cout << "set_parameters start" << std::endl;
  auto results = set_parameters({
    rclcpp::Parameter("sensor_model", os_sensor_model.str()),
    rclcpp::Parameter("return_mode", os_return_mode.str()),
    rclcpp::Parameter("host_ip", sensor_configuration_.host_ip),
    rclcpp::Parameter("sensor_ip", sensor_configuration_.sensor_ip),
    rclcpp::Parameter("frame_id", sensor_configuration_.frame_id),
    rclcpp::Parameter("data_port", sensor_configuration_.data_port),
    rclcpp::Parameter("gnss_port", sensor_configuration_.gnss_port),
    rclcpp::Parameter("scan_phase", sensor_configuration_.scan_phase),
    rclcpp::Parameter("packet_mtu_size", sensor_configuration_.packet_mtu_size),
    rclcpp::Parameter("rotation_speed", sensor_configuration_.rotation_speed),
    rclcpp::Parameter("cloud_min_angle", sensor_configuration_.cloud_min_angle),
    rclcpp::Parameter("cloud_max_angle", sensor_configuration_.cloud_max_angle)  //,
  });
  std::cout << "set_parameters fin" << std::endl;
  return results;
}

void VelodyneHwInterfaceRosWrapper::ReceivePacketMsgCallback(
  const std::unique_ptr<velodyne_msgs::msg::VelodynePacket> msg_packet)
{
  auto t_start = std::chrono::high_resolution_clock::now();

  // take packets out of scan msg
//  std::vector<velodyne_msgs::msg::VelodynePacket> pkt_msgs = scan_msg->packets;

  std::tuple<nebula::drivers::NebulaPointCloudPtr, double> pointcloud_ts =
    driver_ptr_->AccumulatePacketToPointcloud(scan_msg);
  nebula::drivers::NebulaPointCloudPtr pointcloud = std::get<0>(pointcloud_ts);
  double cloud_stamp = std::get<1>(pointcloud_ts);
  if (pointcloud == nullptr) {
    RCLCPP_WARN_STREAM(get_logger(), "Empty cloud parsed.");
    return;
  };
  if (
    nebula_points_pub_->get_subscription_count() > 0 ||
    nebula_points_pub_->get_intra_process_subscription_count() > 0) {
    auto ros_pc_msg_ptr = std::make_unique<sensor_msgs::msg::PointCloud2>();
    pcl::toROSMsg(*pointcloud, *ros_pc_msg_ptr);
    ros_pc_msg_ptr->header.stamp =
      rclcpp::Time(SecondsToChronoNanoSeconds(std::get<1>(pointcloud_ts)).count());
    PublishCloud(std::move(ros_pc_msg_ptr), nebula_points_pub_);
  }
  if (
    aw_points_base_pub_->get_subscription_count() > 0 ||
    aw_points_base_pub_->get_intra_process_subscription_count() > 0) {
    const auto autoware_cloud_xyzi =
      nebula::drivers::convertPointXYZIRCAEDTToPointXYZIR(pointcloud);
    auto ros_pc_msg_ptr = std::make_unique<sensor_msgs::msg::PointCloud2>();
    pcl::toROSMsg(*autoware_cloud_xyzi, *ros_pc_msg_ptr);
    ros_pc_msg_ptr->header.stamp =
      rclcpp::Time(SecondsToChronoNanoSeconds(std::get<1>(pointcloud_ts)).count());
    PublishCloud(std::move(ros_pc_msg_ptr), aw_points_base_pub_);
  }
  if (
    aw_points_ex_pub_->get_subscription_count() > 0 ||
    aw_points_ex_pub_->get_intra_process_subscription_count() > 0) {
    const auto autoware_ex_cloud =
      nebula::drivers::convertPointXYZIRCAEDTToPointXYZIRADT(pointcloud, cloud_stamp);
    auto ros_pc_msg_ptr = std::make_unique<sensor_msgs::msg::PointCloud2>();
    pcl::toROSMsg(*autoware_ex_cloud, *ros_pc_msg_ptr);
    ros_pc_msg_ptr->header.stamp =
      rclcpp::Time(SecondsToChronoNanoSeconds(std::get<1>(pointcloud_ts)).count());
    PublishCloud(std::move(ros_pc_msg_ptr), aw_points_ex_pub_);
  }

  rclcpp::Time time_first_packet(scan_msg->packets.front().stamp);
  std::chrono::system_clock::time_point time_first_packet_chrono(
    std::chrono::nanoseconds(time_first_packet.nanoseconds()));

  std::chrono::system_clock::time_point time_now = std::chrono::system_clock::now();

  // log the dif
  std::chrono::duration<double> time_diff = time_now - time_first_packet_chrono;
  double time_ms = std::chrono::duration_cast<std::chrono::microseconds>(time_diff).count() * 0.001;

  auto registration_id_time_ms = log_channel_->registerValue("time_ms_after", &time_ms);
  log_channel_->takeSnapshot(std::chrono::nanoseconds(count_));
  count_++;
  RCLCPP_INFO(get_logger(), "Count: %d Diff: %f ms", count_, time_ms);

  auto runtime = std::chrono::high_resolution_clock::now() - t_start;
  RCLCPP_DEBUG(get_logger(), "PROFILING {'d_total': %lu, 'n_out': %lu}", runtime.count(), pointcloud->size());
}

void VelodyneHwInterfaceRosWrapper::PublishCloud(
  std::unique_ptr<sensor_msgs::msg::PointCloud2> pointcloud,
  const rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr & publisher)
{
  if (pointcloud->header.stamp.sec < 0) {
    RCLCPP_WARN_STREAM(this->get_logger(), "Timestamp error, verify clock source.");
  }
  pointcloud->header.frame_id = sensor_cfg_ptr_->frame_id;
  publisher->publish(std::move(pointcloud));
}

Status VelodyneHwInterfaceRosWrapper::InitializeDriver(
  std::shared_ptr<drivers::SensorConfigurationBase> sensor_configuration,
  std::shared_ptr<drivers::CalibrationConfigurationBase> calibration_configuration)
{
  // driver should be initialized here with proper decoder
  driver_ptr_ = std::make_shared<drivers::VelodyneDriver>(
    std::static_pointer_cast<drivers::VelodyneSensorConfiguration>(sensor_configuration),
    std::static_pointer_cast<drivers::VelodyneCalibrationConfiguration>(calibration_configuration));
  return driver_ptr_->GetStatus();
}

Status VelodyneHwInterfaceRosWrapper::GetStatus() { return wrapper_status_; }

Status VelodyneHwInterfaceRosWrapper::GetParameters(
  drivers::VelodyneSensorConfiguration & sensor_configuration,
  drivers::VelodyneCalibrationConfiguration & calibration_configuration)
{
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("sensor_model", "");
    sensor_configuration.sensor_model =
      nebula::drivers::SensorModelFromString(this->get_parameter("sensor_model").as_string());
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("return_mode", "", descriptor);
    sensor_configuration.return_mode =
      nebula::drivers::ReturnModeFromString(this->get_parameter("return_mode").as_string());
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("frame_id", "velodyne", descriptor);
    sensor_configuration.frame_id = this->get_parameter("frame_id").as_string();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "Angle where scans begin (degrees, [0.,360.]";
    rcl_interfaces::msg::FloatingPointRange range;
    range.set__from_value(0).set__to_value(360).set__step(0.01);
    descriptor.floating_point_range = {range};
    this->declare_parameter<double>("scan_phase", 0., descriptor);
    sensor_configuration.scan_phase = this->get_parameter("scan_phase").as_double();
  }

  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_STRING;
    descriptor.read_only = true;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<std::string>("calibration_file", "", descriptor);
    calibration_configuration.calibration_file =
      this->get_parameter("calibration_file").as_string();
  }

  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<double>("min_range", 0.3, descriptor);
    sensor_configuration.min_range = this->get_parameter("min_range").as_double();
  }
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<double>("max_range", 300., descriptor);
    sensor_configuration.max_range = this->get_parameter("max_range").as_double();
  }
  double view_direction;
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<double>("view_direction", 0., descriptor);
    view_direction = this->get_parameter("view_direction").as_double();
  }
  double view_width = 2.0 * M_PI;
  {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
    descriptor.read_only = false;
    descriptor.dynamic_typing = false;
    descriptor.additional_constraints = "";
    this->declare_parameter<double>("view_width", 2.0 * M_PI, descriptor);
    view_width = this->get_parameter("view_width").as_double();
  }

  if (sensor_configuration.sensor_model != nebula::drivers::SensorModel::VELODYNE_HDL64) {
    {
      rcl_interfaces::msg::ParameterDescriptor descriptor;
      descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
      descriptor.read_only = false;
      descriptor.dynamic_typing = false;
      descriptor.additional_constraints = "";
      rcl_interfaces::msg::IntegerRange range;
      range.set__from_value(0).set__to_value(360).set__step(1);
      descriptor.integer_range = {range};
      this->declare_parameter<uint16_t>("cloud_min_angle", 0, descriptor);
      sensor_configuration.cloud_min_angle = this->get_parameter("cloud_min_angle").as_int();
    }
    {
      rcl_interfaces::msg::ParameterDescriptor descriptor;
      descriptor.type = rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER;
      descriptor.read_only = false;
      descriptor.dynamic_typing = false;
      descriptor.additional_constraints = "";
      rcl_interfaces::msg::IntegerRange range;
      range.set__from_value(0).set__to_value(360).set__step(1);
      descriptor.integer_range = {range};
      this->declare_parameter<uint16_t>("cloud_max_angle", 360, descriptor);
      sensor_configuration.cloud_max_angle = this->get_parameter("cloud_max_angle").as_int();
    }
  } else {
    double min_angle = fmod(fmod(view_direction + view_width / 2, 2 * M_PI) + 2 * M_PI, 2 * M_PI);
    double max_angle = fmod(fmod(view_direction - view_width / 2, 2 * M_PI) + 2 * M_PI, 2 * M_PI);
    sensor_configuration.cloud_min_angle = 100 * (2 * M_PI - min_angle) * 180 / M_PI + 0.5;
    sensor_configuration.cloud_max_angle = 100 * (2 * M_PI - max_angle) * 180 / M_PI + 0.5;
    if (sensor_configuration.cloud_min_angle == sensor_configuration.cloud_max_angle) {
      // avoid returning empty cloud if min_angle = max_angle
      sensor_configuration.cloud_min_angle = 0;
      sensor_configuration.cloud_max_angle = 36000;
    }
  }

  if (sensor_configuration.sensor_model == nebula::drivers::SensorModel::UNKNOWN) {
    return Status::INVALID_SENSOR_MODEL;
  }
  if (sensor_configuration.return_mode == nebula::drivers::ReturnMode::UNKNOWN) {
    return Status::INVALID_ECHO_MODE;
  }
  if (sensor_configuration.frame_id.empty() || sensor_configuration.scan_phase > 360) {
    return Status::SENSOR_CONFIG_ERROR;
  }

  if (calibration_configuration.calibration_file.empty()) {
    return Status::INVALID_CALIBRATION_FILE;
  } else {
    auto cal_status =
      calibration_configuration.LoadFromFile(calibration_configuration.calibration_file);
    if (cal_status != Status::OK) {
      RCLCPP_ERROR_STREAM(
        this->get_logger(),
        "Given Calibration File: '" << calibration_configuration.calibration_file << "'");
      return cal_status;
    }
  }

  RCLCPP_INFO_STREAM(
    this->get_logger(), "Sensor model: " << sensor_configuration.sensor_model
                                         << ", Return mode: " << sensor_configuration.return_mode
                                         << ", Scan Phase: " << sensor_configuration.scan_phase);
  return Status::OK;
}

RCLCPP_COMPONENTS_REGISTER_NODE(VelodyneHwInterfaceRosWrapper)
}  // namespace ros
}  // namespace nebula
