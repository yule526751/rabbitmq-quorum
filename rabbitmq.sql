CREATE TABLE `rabbitmq_msg` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `exchange_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '交换机名',
    `queue_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '队列名',
    `routing_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '路由键',
    `msg` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '消息',
    `delay` bigint unsigned DEFAULT NULL COMMENT '延迟时间，秒',
    `retry_count` int unsigned NOT NULL DEFAULT '0' COMMENT '重试次数',
    `created_at` datetime NOT NULL,
    `updated_at` datetime NOT NULL,
    `deleted_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_deleted_at` (`deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='rabbitmq消息';


CREATE TABLE `rabbitmq_consume_record` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `message_id` bigint DEFAULT NULL COMMENT 'rabbitmq_msg的id',
    `queue_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '队列名',
    `msg` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '消息',
    `created_at` datetime NOT NULL,
    `updated_at` datetime NOT NULL,
    `deleted_at` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `message_id_uk` (`message_id`,`queue_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;