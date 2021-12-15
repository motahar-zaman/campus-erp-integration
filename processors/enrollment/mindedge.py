from services.mindedge import MindEdgeService

def handle_mindedge_enrollment(erp, profile, data, message_data, enrollment):
    print('*** mindedge ***')
    return
    try:
        store_configuration = store.store_configurations.get(
            external_entity__entity_type='enrollment_config',
            external_entity__entity_name__iexact=erp
        )
    except StoreConfiguration.DoesNotExist:
        save_to_mongo(data={'type': 'erp', 'comment': erp + ' not implemented'},
                      collection='enrollment_status_history')
        return 1

    processor_class = MindEdgeService
    credentials = store_configuration.config_value

    processor_obj = processor_class(credentials, profile, data)

    if not processor_obj.authenticate():
        enrollment.status = 'failed'
        enrollment.save()
        status_data = {'type': 'erp', 'comment': 'authentication_failed', 'data': credentials}

        payment_transaction(payment, store_payment_gateway, 'voidTransaction')
        save_to_mongo(data=status_data, collection='enrollment_status_history')
        return 'auth_failed'

    status_data = {'type': 'erp', 'comment': 'erp_authenticated', 'data': credentials}
    save_to_mongo(data=status_data, collection='enrollment_status_history')
    action = getattr(processor_obj, action)
    resp = action()

    if resp['status'] == 'fail' and not resp['already_enrolled']:
        payment_transaction(payment, store_payment_gateway, 'voidTransaction')
        enrollment.status = 'failed'
        enrollment.save()
        status_data = {'type': 'erp', 'comment': 'enrollment_failed', 'data': credentials}
        return 'enrollment_failed'

    status_data = {'type': 'erp', 'comment': 'enrolled', 'data': resp}
    save_to_mongo(data=status_data, collection='enrollment_status_history')

    # handle already enrolled issue
    if message_data['enrollment_type'] == 'course':
        already_enrolled = False
        try:
            already_enrolled = resp['already_enrolled']
        except KeyError:
            pass

        if already_enrolled:
            # lms says this course was already enrolled to.
            # so there must be another entry in the CourseEnrollment table with the same course for the same profile.
            try:
                old_enrollment = CourseEnrollment.objects.exclude(id=enrollment.id).get(profile=enrollment.profile, course=enrollment.course, section=enrollment.section)
            except CourseEnrollment.DoesNotExist:
                # not found. therefore proceed with the current enrollment obj
                pass
            else:
                # found. so lets delete current enrollment object. and use the found one instead.
                enrollment.delete()
                enrollment = old_enrollment

        enrollment.status = 'success'

        status_data = {'type': 'erp', 'comment': 'lms_created'}
        save_to_mongo(data=status_data, collection='enrollment_status_history')

        LMSAccess.objects.update_or_create(
            course_enrollment=enrollment,
            defaults={
                'student_ref': 'student',
                'lms_access_details': resp,
            }
        )
        return 'success'
        payment_transaction(payment, store_payment_gateway, 'priorAuthCaptureTransaction')
    else:
        # this enrollment is a certificate
        # this is not well implemented
        enrollment.status = 'success'
    enrollment.save()
