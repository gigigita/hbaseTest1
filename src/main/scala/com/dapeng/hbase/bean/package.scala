package com.dapeng.hbase

package object bean {
  case class XF(id:String,
                course_id:String,
                stage_id:String,
                user_id:String,
                lmss_achievements_end:String,
                job_submit_end:String,
                job_submit_time:String,
                curriculum_id:String,
                excelserverrcid:String,
                excelserverrn:String,
                chapter_node:String,
                handin:String,
                score:String,
                handin_num:String,
                last_modified_date:String)
}
