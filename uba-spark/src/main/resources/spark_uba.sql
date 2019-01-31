/*
SQLyog Enterprise v12.09 (64 bit)
MySQL - 5.7.3-m13 : Database - spark_abs
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`spark_abs` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `spark_abs`;

/*Table structure for table `ad_blacklist` */

DROP TABLE IF EXISTS `ad_blacklist`;

CREATE TABLE `ad_blacklist` (
  `user_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `ad_blacklist` */

/*Table structure for table `ad_click_trend` */

DROP TABLE IF EXISTS `ad_click_trend`;

CREATE TABLE `ad_click_trend` (
  `date` varchar(30) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `minute` varchar(30) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `ad_click_trend` */

/*Table structure for table `ad_province_top3` */

DROP TABLE IF EXISTS `ad_province_top3`;

CREATE TABLE `ad_province_top3` (
  `date` varchar(30) DEFAULT NULL,
  `province` varchar(100) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `ad_province_top3` */

/*Table structure for table `ad_stat` */

DROP TABLE IF EXISTS `ad_stat`;

CREATE TABLE `ad_stat` (
  `date` varchar(30) DEFAULT NULL,
  `province` varchar(100) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `ad_stat` */

/*Table structure for table `ad_user_click_count` */

DROP TABLE IF EXISTS `ad_user_click_count`;

CREATE TABLE `ad_user_click_count` (
  `date` varchar(30) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `ad_user_click_count` */

/*Table structure for table `area_top3_product` */

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

/*Data for the table `area_top3_product` */

/*Table structure for table `city_info` */

DROP TABLE IF EXISTS `city_info`;

CREATE TABLE `city_info` (
  `city_id` bigint(11) DEFAULT NULL,
  `city_name` varchar(255) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `city_info` */

insert  into `city_info`(`city_id`,`city_name`,`area`) values (0,'Beijing','China North'),(1,'Shanghai','China East'),(2,'Nanjing','China East'),(3,'Guangzhou','China South'),(4,'Sanya','China South'),(5,'Wuhan','China Middle'),(6,'Changsha','China Middle'),(7,'Xian','China North'),(8,'Chengdu','China South'),(9,'Haerbin','China North'),(10,'XIzang','China North');

/*Table structure for table `page_split_convert_rate` */

DROP TABLE IF EXISTS `page_split_convert_rate`;

CREATE TABLE `page_split_convert_rate` (
  `taskid` int(11) DEFAULT NULL,
  `convert_rate` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `page_split_convert_rate` */

insert  into `page_split_convert_rate`(`taskid`,`convert_rate`) values (3,'1_2=0.1|2_3=0.87|3_4=1.23|4_5=0.92'),(3,'1_2=0.09|2_3=0.96|3_4=1.05|4_5=1.12'),(3,'1_2=0.1|2_3=1.0|3_4=1.22|4_5=0.83');

/*Table structure for table `product_info` */

DROP TABLE IF EXISTS `product_info`;

CREATE TABLE `product_info` (
  `id` int(11) NOT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `info` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `product_info` */

insert  into `product_info`(`id`,`product_name`,`info`) values (0,'product0','{\"product_status\":1}'),(1,'product1','{\"product_status\":1}'),(2,'product2','{\"product_status\":0}'),(3,'product3','{\"product_status\":1}'),(4,'product4','{\"product_status\":1}'),(5,'product5','{\"product_status\":0}'),(6,'product6','{\"product_status\":1}'),(7,'product7','{\"product_status\":1}'),(8,'product8','{\"product_status\":1}'),(9,'product9','{\"product_status\":1}'),(10,'product10','{\"product_status\":1}'),(11,'product11','{\"product_status\":1}'),(12,'product12','{\"product_status\":0}'),(13,'product13','{\"product_status\":1}'),(14,'product14','{\"product_status\":0}'),(15,'product15','{\"product_status\":0}'),(16,'product16','{\"product_status\":0}'),(17,'product17','{\"product_status\":0}'),(18,'product18','{\"product_status\":1}'),(19,'product19','{\"product_status\":0}'),(20,'product20','{\"product_status\":0}'),(21,'product21','{\"product_status\":0}'),(22,'product22','{\"product_status\":1}'),(23,'product23','{\"product_status\":1}'),(24,'product24','{\"product_status\":1}'),(25,'product25','{\"product_status\":1}'),(26,'product26','{\"product_status\":1}'),(27,'product27','{\"product_status\":1}'),(28,'product28','{\"product_status\":1}'),(29,'product29','{\"product_status\":1}'),(30,'product30','{\"product_status\":0}'),(31,'product31','{\"product_status\":1}'),(32,'product32','{\"product_status\":1}'),(33,'product33','{\"product_status\":1}'),(34,'product34','{\"product_status\":0}'),(35,'product35','{\"product_status\":0}'),(36,'product36','{\"product_status\":1}'),(37,'product37','{\"product_status\":1}'),(38,'product38','{\"product_status\":1}'),(39,'product39','{\"product_status\":1}'),(40,'product40','{\"product_status\":1}'),(41,'product41','{\"product_status\":0}'),(42,'product42','{\"product_status\":0}'),(43,'product43','{\"product_status\":1}'),(44,'product44','{\"product_status\":0}'),(45,'product45','{\"product_status\":0}'),(46,'product46','{\"product_status\":1}'),(47,'product47','{\"product_status\":0}'),(48,'product48','{\"product_status\":0}'),(49,'product49','{\"product_status\":1}'),(50,'product50','{\"product_status\":0}'),(51,'product51','{\"product_status\":1}'),(52,'product52','{\"product_status\":1}'),(53,'product53','{\"product_status\":0}'),(54,'product54','{\"product_status\":0}'),(55,'product55','{\"product_status\":1}'),(56,'product56','{\"product_status\":0}'),(57,'product57','{\"product_status\":0}'),(58,'product58','{\"product_status\":0}'),(59,'product59','{\"product_status\":0}'),(60,'product60','{\"product_status\":0}'),(61,'product61','{\"product_status\":0}'),(62,'product62','{\"product_status\":0}'),(63,'product63','{\"product_status\":0}'),(64,'product64','{\"product_status\":0}'),(65,'product65','{\"product_status\":1}'),(66,'product66','{\"product_status\":0}'),(67,'product67','{\"product_status\":1}'),(68,'product68','{\"product_status\":0}'),(69,'product69','{\"product_status\":1}'),(70,'product70','{\"product_status\":0}'),(71,'product71','{\"product_status\":1}'),(72,'product72','{\"product_status\":1}'),(73,'product73','{\"product_status\":0}'),(74,'product74','{\"product_status\":0}'),(75,'product75','{\"product_status\":0}'),(76,'product76','{\"product_status\":0}'),(77,'product77','{\"product_status\":1}'),(78,'product78','{\"product_status\":0}'),(79,'product79','{\"product_status\":0}'),(80,'product80','{\"product_status\":0}'),(81,'product81','{\"product_status\":0}'),(82,'product82','{\"product_status\":1}'),(83,'product83','{\"product_status\":1}'),(84,'product84','{\"product_status\":0}'),(85,'product85','{\"product_status\":1}'),(86,'product86','{\"product_status\":1}'),(87,'product87','{\"product_status\":1}'),(88,'product88','{\"product_status\":1}'),(89,'product89','{\"product_status\":1}'),(90,'product90','{\"product_status\":1}'),(91,'product91','{\"product_status\":0}'),(92,'product92','{\"product_status\":0}'),(93,'product93','{\"product_status\":0}'),(94,'product94','{\"product_status\":0}'),(95,'product95','{\"product_status\":1}'),(96,'product96','{\"product_status\":1}'),(97,'product97','{\"product_status\":1}'),(98,'product98','{\"product_status\":0}'),(99,'product99','{\"product_status\":0}');

/*Table structure for table `session_aggr_stat` */

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

/*Data for the table `session_aggr_stat` */

/*Table structure for table `session_detail` */

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

/*Data for the table `session_detail` */

/*Table structure for table `session_random_extract` */

DROP TABLE IF EXISTS `session_random_extract`;

CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `start_time` varchar(50) DEFAULT NULL,
  `search_keywords` varchar(50) DEFAULT NULL,
  `click_category_id` varchar(1024) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `session_random_extract` */

/*Table structure for table `task` */

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
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

/*Data for the table `task` */

insert  into `task`(`task_id`,`task_name`,`create_time`,`start_time`,`finish_time`,`task_type`,`task_status`,`task_param`) values (1,'findfood','2018-01-01 12:11:10','2018-01-01 12:11:10','2018-01-01 14:11:10','1','1','{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"startAge\":10,\"endAge\":60,\"keywords\":\"火锅,美女,面包\"}'),(2,'tech','2018-01-01 12:11:10','2018-01-01 12:11:10','2018-01-01 14:11:10','1','1','{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"startAge\":30,\"endAge\":50,\"keywords\":\"java\"}'),(3,'page_convert','2018-01-01 12:11:10','2018-01-01 12:11:10','2018-01-01 14:11:10','2','1','{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\",\"targetPageFlow\":\"1,2,3,4,5\"}'),(4,'hotproduct','2018-01-01 12:11:10','2018-01-01 12:11:10','2018-01-01 14:11:10','3','1','{\"startDate\":\"2018-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\"}'),(5,'useractive','2018-01-01 12:11:10','2018-01-01 12:11:10','2018-01-01 12:11:10','4','1','{\"startDate\":\"2016-10-14 10:00:00\",\"endDate\":\"2019-11-21 10:00:00\"}');

/*Table structure for table `test_user` */

DROP TABLE IF EXISTS `test_user`;

CREATE TABLE `test_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

/*Data for the table `test_user` */

insert  into `test_user`(`id`,`name`,`age`) values (1,'张三',25),(4,'李四',26),(5,'王二',28),(6,'麻子',30),(7,'王五',35);

/*Table structure for table `top10_category` */

DROP TABLE IF EXISTS `top10_category`;

CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `order_count` int(11) DEFAULT NULL,
  `pay_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `top10_category` */

/*Table structure for table `top10_category_session` */

DROP TABLE IF EXISTS `top10_category_session`;

CREATE TABLE `top10_category_session` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `top10_category_session` */

/*Table structure for table `top10_session` */

DROP TABLE IF EXISTS `top10_session`;

CREATE TABLE `top10_session` (
  `task_id` int(11) DEFAULT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `top10_session` */

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
