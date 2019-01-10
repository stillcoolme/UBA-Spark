/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50703
Source Host           : localhost:3306
Source Database       : spark_abs

Target Server Type    : MYSQL
Target Server Version : 50703
File Encoding         : 65001

Date: 2018-12-10 20:42:22
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for area_top3_product
-- ----------------------------
DROP TABLE IF EXISTS `area_top3_product`;
CREATE TABLE `area_top3_product` (
  `task_id` int(11) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL,
  `area_level` varchar(255) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `city_infos` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `product_status` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of area_top3_product
-- ----------------------------

-- ----------------------------
-- Table structure for city_info
-- ----------------------------
DROP TABLE IF EXISTS `city_info`;
CREATE TABLE `city_info` (
  `city_id` bigint(11) DEFAULT NULL,
  `city_name` varchar(255) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of city_info
-- ----------------------------
INSERT INTO `city_info` VALUES ('0', 'Beijing', 'China North');
INSERT INTO `city_info` VALUES ('1', 'Shanghai', 'China East');
INSERT INTO `city_info` VALUES ('2', 'Nanjing', 'China East');
INSERT INTO `city_info` VALUES ('3', 'Guangzhou', 'China South');
INSERT INTO `city_info` VALUES ('4', 'Sanya', 'China South');
INSERT INTO `city_info` VALUES ('5', 'Wuhan', 'China Middle');
INSERT INTO `city_info` VALUES ('6', 'Changsha', 'China Middle');
INSERT INTO `city_info` VALUES ('7', 'Xian', 'China North');
INSERT INTO `city_info` VALUES ('8', 'Chengdu', 'China South');
INSERT INTO `city_info` VALUES ('9', 'Haerbin', 'China North');
INSERT INTO `city_info` VALUES ('10', 'XIzang', 'China North');

-- ----------------------------
-- Table structure for page_split_convert_rate
-- ----------------------------
DROP TABLE IF EXISTS `page_split_convert_rate`;
CREATE TABLE `page_split_convert_rate` (
  `taskid` int(11) DEFAULT NULL,
  `convert_rate` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of page_split_convert_rate
-- ----------------------------
INSERT INTO `page_split_convert_rate` VALUES ('3', '1_2=0.1|2_3=0.87|3_4=1.23|4_5=0.92');
INSERT INTO `page_split_convert_rate` VALUES ('3', '1_2=0.09|2_3=0.96|3_4=1.05|4_5=1.12');
INSERT INTO `page_split_convert_rate` VALUES ('3', '1_2=0.1|2_3=1.0|3_4=1.22|4_5=0.83');

-- ----------------------------
-- Table structure for product_info
-- ----------------------------
DROP TABLE IF EXISTS `product_info`;
CREATE TABLE `product_info` (
  `id` int(11) NOT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `info` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of product_info
-- ----------------------------
INSERT INTO `product_info` VALUES ('0', 'product0', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('1', 'product1', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('2', 'product2', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('3', 'product3', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('4', 'product4', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('5', 'product5', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('6', 'product6', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('7', 'product7', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('8', 'product8', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('9', 'product9', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('10', 'product10', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('11', 'product11', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('12', 'product12', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('13', 'product13', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('14', 'product14', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('15', 'product15', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('16', 'product16', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('17', 'product17', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('18', 'product18', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('19', 'product19', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('20', 'product20', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('21', 'product21', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('22', 'product22', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('23', 'product23', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('24', 'product24', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('25', 'product25', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('26', 'product26', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('27', 'product27', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('28', 'product28', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('29', 'product29', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('30', 'product30', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('31', 'product31', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('32', 'product32', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('33', 'product33', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('34', 'product34', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('35', 'product35', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('36', 'product36', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('37', 'product37', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('38', 'product38', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('39', 'product39', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('40', 'product40', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('41', 'product41', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('42', 'product42', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('43', 'product43', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('44', 'product44', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('45', 'product45', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('46', 'product46', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('47', 'product47', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('48', 'product48', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('49', 'product49', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('50', 'product50', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('51', 'product51', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('52', 'product52', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('53', 'product53', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('54', 'product54', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('55', 'product55', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('56', 'product56', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('57', 'product57', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('58', 'product58', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('59', 'product59', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('60', 'product60', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('61', 'product61', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('62', 'product62', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('63', 'product63', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('64', 'product64', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('65', 'product65', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('66', 'product66', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('67', 'product67', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('68', 'product68', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('69', 'product69', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('70', 'product70', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('71', 'product71', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('72', 'product72', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('73', 'product73', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('74', 'product74', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('75', 'product75', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('76', 'product76', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('77', 'product77', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('78', 'product78', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('79', 'product79', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('80', 'product80', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('81', 'product81', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('82', 'product82', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('83', 'product83', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('84', 'product84', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('85', 'product85', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('86', 'product86', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('87', 'product87', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('88', 'product88', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('89', 'product89', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('90', 'product90', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('91', 'product91', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('92', 'product92', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('93', 'product93', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('94', 'product94', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('95', 'product95', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('96', 'product96', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('97', 'product97', '{\"product_status\":1}');
INSERT INTO `product_info` VALUES ('98', 'product98', '{\"product_status\":0}');
INSERT INTO `product_info` VALUES ('99', 'product99', '{\"product_status\":0}');

-- ----------------------------
-- Table structure for session_aggr_stat
-- ----------------------------
DROP TABLE IF EXISTS `session_aggr_stat`;
CREATE TABLE `session_aggr_stat` (
  `task_id` int(11) NOT NULL,
  `session_count` int(11) DEFAULT NULL,
  `1s_3s` double DEFAULT NULL,
  `4s_6s` double DEFAULT NULL,
  `7s_9s` double DEFAULT NULL,
  `10s_30s` double DEFAULT NULL,
  `30s_60s` double DEFAULT NULL,
  `1m_3m` double DEFAULT NULL,
  `3m_10m` double DEFAULT NULL,
  `10m_30m` double DEFAULT NULL,
  `30m` double DEFAULT NULL,
  `1_3` double DEFAULT NULL,
  `4_6` double DEFAULT NULL,
  `7_9` double DEFAULT NULL,
  `10_30` double DEFAULT NULL,
  `30_60` double DEFAULT NULL,
  `60` double DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_aggr_stat
-- ----------------------------

-- ----------------------------
-- Table structure for session_detail
-- ----------------------------
DROP TABLE IF EXISTS `session_detail`;
CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `search_keyword` varchar(255) DEFAULT NULL,
  `click_category_id` int(11) DEFAULT NULL,
  `click_product_id` int(11) DEFAULT NULL,
  `order_category_ids` varchar(255) DEFAULT NULL,
  `order_product_ids` varchar(255) DEFAULT NULL,
  `pay_category_ids` varchar(255) DEFAULT NULL,
  `pay_product_ids` varchar(255) DEFAULT NULL,
  KEY `task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_detail
-- ----------------------------

-- ----------------------------
-- Table structure for session_random_extract
-- ----------------------------
DROP TABLE IF EXISTS `session_random_extract`;
CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `start_time` varchar(50) DEFAULT NULL,
  `search_keywords` varchar(50) DEFAULT NULL,
  `click_category_id` varchar(1024) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_random_extract
-- ----------------------------

-- ----------------------------
-- Table structure for task
-- ----------------------------
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `task_name` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `finish_time` varchar(255) DEFAULT NULL,
  `task_type` varchar(255) DEFAULT NULL,
  `task_status` varchar(255) DEFAULT NULL,
  `task_param` text,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of task
-- ----------------------------
INSERT INTO `task` VALUES ('1', 'findfood', '2018-01-01 12:11:10', '2018-01-01 12:11:10', '2018-01-01 14:11:10', '1', '1', '{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"startAge\":10,\"endAge\":60,\"keywords\":\"火锅,美女,面包\"}');
INSERT INTO `task` VALUES ('2', 'tech', '2018-01-01 12:11:10', '2018-01-01 12:11:10', '2018-01-01 14:11:10', '1', '1', '{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"startAge\":30,\"endAge\":50,\"keywords\":\"java\"}');
INSERT INTO `task` VALUES ('3', 'page_convert', '2018-01-01 12:11:10', '2018-01-01 12:11:10', '2018-01-01 14:11:10', '2', '1', '{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"targetPageFlow\":\"1,2,3,4,5\"}');
INSERT INTO `task` VALUES ('4', 'hotproduct', '2018-01-01 12:11:10', '2018-01-01 12:11:10', '2018-01-01 14:11:10', '3', '1', '{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\"}');

-- ----------------------------
-- Table structure for test_user
-- ----------------------------
DROP TABLE IF EXISTS `test_user`;
CREATE TABLE `test_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of test_user
-- ----------------------------
INSERT INTO `test_user` VALUES ('1', '张三', '25');
INSERT INTO `test_user` VALUES ('4', '李四', '26');
INSERT INTO `test_user` VALUES ('5', '王二', '28');
INSERT INTO `test_user` VALUES ('6', '麻子', '30');
INSERT INTO `test_user` VALUES ('7', '王五', '35');

-- ----------------------------
-- Table structure for top10_category
-- ----------------------------
DROP TABLE IF EXISTS `top10_category`;
CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `order_count` int(11) DEFAULT NULL,
  `pay_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of top10_category
-- ----------------------------

-- ----------------------------
-- Table structure for top10_category_session
-- ----------------------------
DROP TABLE IF EXISTS `top10_category_session`;
CREATE TABLE `top10_category_session` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of top10_category_session
-- ----------------------------

-- ----------------------------
-- Table structure for top10_session
-- ----------------------------
DROP TABLE IF EXISTS `top10_session`;
CREATE TABLE `top10_session` (
  `task_id` int(11) DEFAULT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of top10_session
-- ----------------------------
